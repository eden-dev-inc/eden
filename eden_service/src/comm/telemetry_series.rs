//! Batch, time-bucketed metric series for the dashboard.
//!
//! `GET /api/v1/analytics/series` returns several metrics' time-bucketed,
//! aggregated values in ONE compact, columnar response — purpose-built for the
//! Leptos dashboard's panels. Unlike `telemetry_analytics::export` (raw per-row
//! rows, one domain per call), this aggregates server-side with `toStartOfInterval`
//! GROUP BY, returns one shared `{t0, step, n}` time grid + flat `values[]` arrays
//! per series (≈10× smaller), and (in later steps) layers Redis + single-flight +
//! ETag/304 + incremental `since` on top.
//!
//! Correctness notes:
//! - "sum"/Counter metrics are CUMULATIVE per node (exported as running totals),
//!   so per-bucket value = per-node window-diff (last cumulative this bucket minus
//!   last cumulative previous bucket, reset-guarded) summed across nodes — never a
//!   naïve `sum(value)`.
//! - UpDownCounters = latest per label series summed per bucket. Gauges =
//!   `avg(value)` per bucket. Histograms = merge `bucket_counts` then interpolate
//!   p50/p95/p99.
//! - `eden.analytics.endpoint.*` come from the pre-aggregated `endpoint_metrics`
//!   snapshot table (org column `organization_uuid`, time column `snapshot_time`).

use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::comm::telemetry_analytics::{
    clickhouse_time, escape_clickhouse_string, optional_param, parse_optional_time, parse_range_secs, validate_map_key,
};
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use chrono::{DateTime, Duration, Utc};
use database::db::cache_ops::CacheOps;
use eden_core::auth::ParsedJwt;
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{EdenUuid, EndpointUuid, parse_kind_uuid};
use eden_core::telemetry::{
    FastSpan, FastSpanKind, FastSpanStatus, FastSpanValue, LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL, TRAFFIC_CLASS_INTERNAL,
};
use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use telemetry_extensions_macro::with_telemetry;

#[cfg(embedded_db)]
type AnalyticsSeriesClient = database::db::lib::ClickhousePooledConnection;
#[cfg(not(embedded_db))]
type AnalyticsSeriesClient = clickhouse::Client;

const DEFAULT_BUCKETS: usize = 32;
const MAX_BUCKETS: usize = 240;
const MAX_METRICS: usize = 32;
const DEFAULT_RANGE_SECS: i64 = 60 * 60;
const MAX_RANGE_SECS: i64 = 365 * 24 * 60 * 60;

/// Generic per-domain telemetry metric tables (uniform schema: metric_name +
/// value/count/sum + bucket arrays + labels). A metric lives in exactly one of
/// these (chosen at write time by its domain); we UNION across all and filter by
/// `metric_name`, which is cheap (LowCardinality + part of the ORDER BY key) and
/// avoids a brittle per-metric→table map.
const GENERIC_TABLES: &[&str] = &[
    "analytics.analytics",
    "analytics.eden",
    "analytics.iam",
    "analytics.endpoint",
    "analytics.metadata",
    "analytics.proxy",
    "analytics.snapshot",
    "analytics.workload",
    "analytics.validator",
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Strategy {
    Counter,
    UpDownCounter,
    Gauge,
    Histogram,
}

/// How to source + aggregate one requested metric.
enum Source {
    /// UNION across `GENERIC_TABLES` filtered by `metric_name`, aggregated per kind.
    Generic { metric_name: String, strategy: Strategy },
    /// Pre-aggregated snapshot column from `analytics.endpoint_metrics`.
    Snapshot { value_expr: &'static str },
    /// Aggregate from an analytics table that does not use the generic metric schema.
    Table {
        table: &'static str,
        time_col: &'static str,
        value_expr: &'static str,
        endpoint_col: Option<&'static str>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TrafficClassFilter {
    All,
    External,
    Internal,
}

impl TrafficClassFilter {
    fn parse(raw: Option<&str>) -> Result<Self, actix_web::Error> {
        match raw.unwrap_or("all") {
            "" | "all" => Ok(Self::All),
            "external" => Ok(Self::External),
            "internal" => Ok(Self::Internal),
            _ => Err(actix_web::error::ErrorBadRequest("invalid traffic_class")),
        }
    }

    fn cache_key(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::External => "external",
            Self::Internal => "internal",
        }
    }

    fn generic_where_clause(self) -> Option<String> {
        match self {
            Self::All => None,
            Self::External => Some(format!("labels['{LABEL_TRAFFIC_CLASS}'] = '{TRAFFIC_CLASS_EXTERNAL}'")),
            Self::Internal => Some(format!("labels['{LABEL_TRAFFIC_CLASS}'] = '{TRAFFIC_CLASS_INTERNAL}'")),
        }
    }
}

struct Requested {
    /// Dashboard metric name (echoed back in the response).
    name: String,
    /// Dashboard kind string, echoed back so the client renders correctly.
    kind: String,
    source: Source,
}

/// Map an `eden.analytics.endpoint.<leaf>` name to a precomputed snapshot column
/// expression, or `None` to fall through to the generic path.
fn endpoint_metrics_expr(name: &str) -> Option<&'static str> {
    match name.strip_prefix("eden.analytics.endpoint.")? {
        "ops_per_sec" => Some("avg(ops_per_sec)"),
        "commands" => Some("toFloat64(sum(ifNull(total_commands, 0)))"),
        "errors" => Some("toFloat64(sum(ifNull(total_errors, 0)))"),
        "connected_clients" => Some("avg(connected_clients)"),
        // dashboard wants ms; the column is microseconds.
        "latency_p99_ms" => Some("avg(latency_p99_us) / 1000.0"),
        _ => None,
    }
}

fn analytics_table_source(name: &str) -> Option<Source> {
    let source = match name {
        "eden.analytics.command.requests" | "eden.analytics.command.requests_by_command" => Source::Table {
            table: "analytics.command_rollups",
            time_col: "window_start",
            value_expr: "toFloat64(sum(request_count))",
            endpoint_col: Some("endpoint_uuid"),
        },
        "eden.analytics.command.errors" => Source::Table {
            table: "analytics.command_rollups",
            time_col: "window_start",
            value_expr: "toFloat64(sum(error_count))",
            endpoint_col: Some("endpoint_uuid"),
        },
        "eden.analytics.command.avg_latency_ms" => Source::Table {
            table: "analytics.command_rollups",
            time_col: "window_start",
            value_expr: "if(sum(latency_sample_count) = 0, NULL, sum(latency_sample_sum_us) / sum(latency_sample_count) / 1000.0)",
            endpoint_col: Some("endpoint_uuid"),
        },
        "eden.analytics.api.requests" => Source::Table {
            table: "analytics.api_usage_history",
            time_col: "request_time",
            value_expr: "toFloat64(count())",
            endpoint_col: Some("endpoint_uuid"),
        },
        "eden.analytics.api.server_errors" => Source::Table {
            table: "analytics.api_usage_history",
            time_col: "request_time",
            value_expr: "toFloat64(countIf(http_status >= 500 AND http_status < 600))",
            endpoint_col: Some("endpoint_uuid"),
        },
        "eden.analytics.api.avg_latency_ms" => Source::Table {
            table: "analytics.api_usage_history",
            time_col: "request_time",
            value_expr: "avg(latency_us) / 1000.0",
            endpoint_col: Some("endpoint_uuid"),
        },
        "eden.analytics.anti_patterns.count" => Source::Table {
            table: "analytics.anti_patterns",
            time_col: "detected_at",
            value_expr: "toFloat64(sum(occurrence_count))",
            endpoint_col: Some("endpoint_uuid"),
        },
        "eden.analytics.snapshot.count_by_status" => Source::Table {
            table: "analytics.infrastructure_snapshots",
            time_col: "snapshot_time",
            value_expr: "toFloat64(count())",
            endpoint_col: Some("source_endpoint_uuid"),
        },
        "eden.analytics.snapshot.bytes_written_total" => Source::Table {
            table: "analytics.infrastructure_snapshots",
            time_col: "snapshot_time",
            value_expr: "toFloat64(sum(bytes_written_total))",
            endpoint_col: Some("source_endpoint_uuid"),
        },
        "eden.analytics.snapshot.avg_duration_secs" => Source::Table {
            table: "analytics.infrastructure_snapshots",
            time_col: "snapshot_time",
            value_expr: "avg(duration_secs)",
            endpoint_col: Some("source_endpoint_uuid"),
        },
        _ => return None,
    };
    Some(source)
}

fn strategy_for_kind(kind: &str) -> Strategy {
    match kind {
        "Counter" => Strategy::Counter,
        "UpDownCounter" => Strategy::UpDownCounter,
        "Histogram" => Strategy::Histogram,
        // Gauge and anything else.
        _ => Strategy::Gauge,
    }
}

/// Resolve a `name|kind` request entry into a query plan.
fn resolve(name: &str, kind: &str) -> Requested {
    let source = if let Some(value_expr) = endpoint_metrics_expr(name) {
        Source::Snapshot { value_expr }
    } else if let Some(source) = analytics_table_source(name) {
        source
    } else {
        Source::Generic {
            metric_name: name.to_string(),
            strategy: strategy_for_kind(kind),
        }
    };
    Requested { name: name.to_string(), kind: kind.to_string(), source }
}

/// Parsed request window + scope.
struct SeriesRequest {
    metrics: Vec<Requested>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    buckets: usize,
    step_secs: i64,
    /// Aligned epoch-seconds of the first bucket (floor(from / step) * step).
    grid_start_secs: i64,
    scope_label: Option<(&'static str, String)>,
    /// Endpoint uuid to RBAC-check (when scope is an endpoint).
    scope_endpoint: Option<EndpointUuid>,
    /// Optional traffic-origin filter for generic telemetry rows.
    traffic_class: TrafficClassFilter,
    /// Optional gateway endpoint-kind/protocol filter for generic telemetry rows.
    endpoint_kind: Option<String>,
}

fn parse_request(params: &HashMap<String, String>) -> Result<SeriesRequest, actix_web::Error> {
    // metrics=name|kind,name|kind,...
    let raw = optional_param(params, "metrics").ok_or_else(|| actix_web::error::ErrorBadRequest("metrics is required"))?;
    let mut metrics = Vec::new();
    for entry in raw.split(',').filter(|s| !s.is_empty()) {
        let (name, kind) = entry.split_once('|').unwrap_or((entry, "Gauge"));
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        validate_metric_name(name)?;
        metrics.push(resolve(name, kind.trim()));
        if metrics.len() > MAX_METRICS {
            return Err(actix_web::error::ErrorBadRequest("too many metrics requested"));
        }
    }
    if metrics.is_empty() {
        return Err(actix_web::error::ErrorBadRequest("no valid metrics requested"));
    }

    let to = parse_optional_time(params, "to")?.unwrap_or_else(Utc::now);
    let to_ts = to.timestamp();
    // The intended full window (independent of `since`) fixes the step so the grid
    // stays aligned whether we fetch the whole window or just an incremental tail.
    let range_secs = match parse_optional_time(params, "from")? {
        Some(from) => (to - from).num_seconds(),
        None => parse_range_secs(optional_param(params, "range"))?.unwrap_or(DEFAULT_RANGE_SECS),
    };
    if range_secs <= 0 {
        return Err(actix_web::error::ErrorBadRequest("from must be before to"));
    }
    if range_secs > MAX_RANGE_SECS {
        return Err(actix_web::error::ErrorBadRequest("range too large"));
    }
    let buckets = optional_param(params, "buckets").and_then(|v| v.parse::<usize>().ok()).unwrap_or(DEFAULT_BUCKETS).clamp(1, MAX_BUCKETS);
    let step_secs = ((range_secs + buckets as i64 - 1) / buckets as i64).max(1);
    let full_from_ts = to_ts - range_secs;
    // Incremental cursor: only return buckets at/after `since` (epoch ms).
    let since_secs = optional_param(params, "since").and_then(|v| v.parse::<i64>().ok()).map(|ms| ms / 1000);
    let query_from_ts = since_secs.map(|s| full_from_ts.max(s)).unwrap_or(full_from_ts);
    let grid_start_secs = (query_from_ts / step_secs) * step_secs;
    let n = (((to_ts - grid_start_secs) / step_secs) as usize).clamp(1, buckets);
    let from = DateTime::from_timestamp(grid_start_secs, 0).unwrap_or_else(|| to - Duration::seconds(range_secs));

    // scope: scope_kind + scope_id → label filter (+ RBAC entity when endpoint).
    let scope_label = match (optional_param(params, "scope_kind"), optional_param(params, "scope_id")) {
        (Some(kind), Some(id)) if !id.is_empty() => {
            validate_map_key(id).map_err(|_| actix_web::error::ErrorBadRequest("invalid scope_id"))?;
            let label = match kind {
                "endpoint" | "function" => "endpoint_uuid",
                "interlay" => "interlay_uuid",
                "migration" => "migration_uuid",
                "api" => "api_id",
                _ => return Err(actix_web::error::ErrorBadRequest("invalid scope_kind")),
            };
            Some((label, id.to_string()))
        }
        _ => None,
    };
    let scope_endpoint = match (optional_param(params, "scope_kind"), optional_param(params, "scope_id")) {
        (Some("endpoint"), Some(id)) if !id.is_empty() => parse_kind_uuid::<EndpointUuid>(id).ok(),
        _ => None,
    };
    let traffic_class = TrafficClassFilter::parse(optional_param(params, "traffic_class"))?;
    let endpoint_kind = parse_endpoint_kind_filter(optional_param(params, "endpoint_kind"))?;

    Ok(SeriesRequest {
        metrics,
        from,
        to,
        buckets: n,
        step_secs,
        grid_start_secs,
        scope_label,
        scope_endpoint,
        traffic_class,
        endpoint_kind,
    })
}

fn validate_metric_name(name: &str) -> Result<(), actix_web::Error> {
    if !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.')) {
        Ok(())
    } else {
        Err(actix_web::error::ErrorBadRequest(format!("invalid metric name: {name}")))
    }
}

fn parse_endpoint_kind_filter(raw: Option<&str>) -> Result<Option<String>, actix_web::Error> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty() && *value != "all") else {
        return Ok(None);
    };
    let endpoint_kind = raw.to_ascii_lowercase();
    validate_map_key(&endpoint_kind).map_err(|_| actix_web::error::ErrorBadRequest("invalid endpoint_kind"))?;
    Ok(Some(endpoint_kind))
}

// ── SQL builders ──

fn metric_name_filter(metric_name: &str) -> String {
    let candidates = metric_name_candidates(metric_name);
    if candidates.len() == 1 {
        return format!("metric_name = '{}'", escape_clickhouse_string(&candidates[0]));
    }
    let names = candidates.iter().map(|candidate| format!("'{}'", escape_clickhouse_string(candidate))).collect::<Vec<_>>().join(", ");
    format!("metric_name IN ({names})")
}

fn metric_name_candidates(metric_name: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    push_metric_candidate(&mut candidates, metric_name.to_string());
    if let Some(alias) = prometheus_metric_alias(metric_name) {
        push_metric_candidate(&mut candidates, alias);
    }
    for alias in gateway_redis_metric_aliases(metric_name) {
        push_metric_candidate(&mut candidates, alias.clone());
        if let Some(raw_alias) = prometheus_metric_alias(&alias) {
            push_metric_candidate(&mut candidates, raw_alias);
        }
    }
    candidates
}

fn push_metric_candidate(candidates: &mut Vec<String>, candidate: String) {
    if !candidates.iter().any(|existing| existing == &candidate) {
        candidates.push(candidate);
    }
}

fn gateway_redis_metric_aliases(metric_name: &str) -> Vec<String> {
    const PAIRS: &[(&str, &str)] = &[
        ("gateway.redis.commands_total", "gateway.commands_total"),
        ("gateway.redis.command_duration_microseconds", "gateway.command_duration_microseconds"),
        ("gateway.redis.command_end_to_end_microseconds", "gateway.command_end_to_end_microseconds"),
        (
            "gateway.redis.command_endpoint_duration_microseconds",
            "gateway.command_endpoint_duration_microseconds",
        ),
        (
            "gateway.redis.command_overhead_duration_microseconds",
            "gateway.command_overhead_duration_microseconds",
        ),
    ];
    PAIRS
        .iter()
        .filter_map(|(canonical, legacy)| {
            if metric_name == *canonical {
                Some((*legacy).to_string())
            } else if metric_name == *legacy {
                Some((*canonical).to_string())
            } else {
                None
            }
        })
        .collect()
}

fn prometheus_metric_alias(metric_name: &str) -> Option<String> {
    if let Some(rest) = metric_name.strip_prefix("gateway.redis.") {
        return Some(format!("gateway_redis_{rest}"));
    }
    if metric_name == "eden.request_sent" {
        return Some("eden_request_count".to_string());
    }
    if let Some(rest) = metric_name.strip_prefix("eden.llm.gateway.") {
        return Some(format!("eden_llm_gateway_{rest}"));
    }
    if let Some(rest) = metric_name.strip_prefix("eden.llm.") {
        return Some(format!("eden_llm_{rest}"));
    }
    if let Some(rest) = metric_name.strip_prefix("eden.endpoint.") {
        return Some(format!("eden.endpoint_{rest}"));
    }
    if let Some(rest) = metric_name.strip_prefix("eden.iam.") {
        return Some(format!("eden.iam_{rest}"));
    }
    if let Some(rest) = metric_name.strip_prefix("eden.validator.") {
        return Some(format!("eden.validator_{rest}"));
    }
    if let Some(rest) = metric_name.strip_prefix("eden.") {
        return Some(format!("eden_{rest}"));
    }
    for prefix in ["analytics", "gateway", "redis", "snapshot", "workload"] {
        let dotted = format!("{prefix}.");
        if let Some(rest) = metric_name.strip_prefix(&dotted) {
            return Some(format!("{prefix}_{rest}"));
        }
    }
    None
}

/// Common time + org + optional scope-label WHERE for the generic tables.
fn generic_where(org: &str, metric_name: &str, req: &SeriesRequest) -> String {
    generic_where_with_bounds(org, metric_name, req, req.from, req.to)
}

fn generic_where_with_bounds(org: &str, metric_name: &str, req: &SeriesRequest, from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    let metric_filter = metric_name_filter(metric_name);
    let mut w = format!(
        "organization_uuid = '{}' AND {metric_filter} AND timestamp >= toDateTime64('{}', 3, 'UTC') AND timestamp < toDateTime64('{}', 3, 'UTC')",
        escape_clickhouse_string(org),
        clickhouse_time(from),
        clickhouse_time(to),
    );
    if let Some((label, id)) = &req.scope_label {
        w.push_str(&format!(" AND labels['{}'] = '{}'", label, escape_clickhouse_string(id)));
    }
    if let Some(filter) = req.traffic_class.generic_where_clause() {
        w.push_str(" AND ");
        w.push_str(&filter);
    }
    if let Some(endpoint_kind) = &req.endpoint_kind {
        w.push_str(&format!(" AND labels['endpoint_kind'] = '{}'", escape_clickhouse_string(endpoint_kind)));
    }
    w
}

/// UNION ALL of the generic domain tables, selecting the columns every strategy
/// may need. ClickHouse skips tables with no matching `metric_name` rows.
fn generic_union(org: &str, metric_name: &str, req: &SeriesRequest) -> String {
    let where_clause = generic_where(org, metric_name, req);
    generic_union_with_where(&where_clause)
}

fn generic_counter_union_with_bounds(org: &str, metric_name: &str, req: &SeriesRequest, from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    let where_clause = generic_where_with_bounds(org, metric_name, req, from, to);
    GENERIC_TABLES
        .iter()
        .map(|t| format!("SELECT timestamp, node_uuid, value FROM {t} WHERE {where_clause}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn generic_value_union(org: &str, metric_name: &str, req: &SeriesRequest) -> String {
    let where_clause = generic_where(org, metric_name, req);
    GENERIC_TABLES
        .iter()
        .map(|t| format!("SELECT timestamp, node_uuid, labels, value FROM {t} WHERE {where_clause}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn generic_union_with_where(where_clause: &str) -> String {
    GENERIC_TABLES
        .iter()
        .map(|t| format!("SELECT timestamp, node_uuid, value, count, bucket_bounds, bucket_counts FROM {t} WHERE {where_clause}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn bucket_expr(time_col: &str, step: i64) -> String {
    format!("toUInt32(toUnixTimestamp(toStartOfInterval({time_col}, INTERVAL {step} SECOND)))")
}

fn counter_sql(org: &str, metric_name: &str, req: &SeriesRequest) -> String {
    let baseline_from = req.from - Duration::seconds(req.step_secs);
    let union = generic_counter_union_with_bounds(org, metric_name, req, baseline_from, req.to);
    let bucket = bucket_expr("timestamp", req.step_secs);
    // per-(node,bucket) last cumulative → per-node window diff (reset-guarded) → sum across nodes.
    format!(
        "SELECT bucket_ts, toNullable(sum(delta)) AS value FROM (\
           SELECT node_uuid, bucket_ts, \
             if(raw < 0, last_val, raw) AS delta FROM (\
               SELECT node_uuid, bucket_ts, last_val, \
                 last_val - lagInFrame(last_val, 1, 0.0) OVER (PARTITION BY node_uuid ORDER BY bucket_ts) AS raw FROM (\
                   SELECT node_uuid, {bucket} AS bucket_ts, argMax(ifNull(value, 0.0), timestamp) AS last_val \
                   FROM ({union}) GROUP BY node_uuid, bucket_ts\
                 )\
             )\
         ) WHERE bucket_ts >= {} GROUP BY bucket_ts ORDER BY bucket_ts SETTINGS use_query_cache = 1",
        req.grid_start_secs
    )
}

fn up_down_counter_sql(org: &str, metric_name: &str, req: &SeriesRequest) -> String {
    let union = generic_value_union(org, metric_name, req);
    let bucket = bucket_expr("timestamp", req.step_secs);
    format!(
        "SELECT bucket_ts, toNullable(sum(last_val)) AS value FROM (\
           SELECT node_uuid, cityHash64(toString(labels)) AS labels_hash, {bucket} AS bucket_ts, \
                  argMax(ifNull(value, 0.0), timestamp) AS last_val \
           FROM ({union}) WHERE isNotNull(value) GROUP BY node_uuid, labels_hash, bucket_ts\
         ) GROUP BY bucket_ts ORDER BY bucket_ts SETTINGS use_query_cache = 1"
    )
}

fn gauge_sql(org: &str, metric_name: &str, req: &SeriesRequest) -> String {
    let union = generic_union(org, metric_name, req);
    let bucket = bucket_expr("timestamp", req.step_secs);
    // `avg` already skips NULLs; an explicit `WHERE isNotNull(value)` would collide
    // with the `avg(value) AS value` alias (ILLEGAL_AGGREGATION on ClickHouse).
    format!(
        "SELECT {bucket} AS bucket_ts, avg(value) AS value FROM ({union}) GROUP BY bucket_ts ORDER BY bucket_ts SETTINGS use_query_cache = 1"
    )
}

fn histogram_sql(org: &str, metric_name: &str, req: &SeriesRequest) -> String {
    let baseline_from = req.from - Duration::seconds(req.step_secs);
    let where_clause = generic_where_with_bounds(org, metric_name, req, baseline_from, req.to);
    let union = generic_union_with_where(&where_clause);
    let bucket = bucket_expr("timestamp", req.step_secs);
    format!(
        "SELECT bucket_ts, \
             if(zero_total > 0, arrayConcat([zero_total], bucket_deltas), bucket_deltas) AS counts, \
             if(zero_total > 0 AND length(bounds) > 0, arrayConcat([0.0], bounds), bounds) AS bounds, \
             zero_total + toUInt64(arraySum(bucket_deltas)) AS total \
         FROM (\
             SELECT bucket_ts, bounds, \
                    sumForEach(delta_counts) AS bucket_deltas, \
                    toUInt64(sum(greatest(toInt64(delta_count) - toInt64(arraySum(delta_counts)), 0))) AS zero_total \
             FROM (\
                 SELECT bucket_ts, bounds, \
                        if(length(prev_counts) = length(last_counts), \
                           arrayMap((current, previous) -> if(current >= previous, toUInt64(current - previous), current), last_counts, prev_counts), \
                           last_counts) AS delta_counts, \
                        if(last_count >= prev_count, toUInt64(last_count - prev_count), last_count) AS delta_count \
                 FROM (\
                     SELECT node_uuid, bucket_ts, bounds, last_count, last_counts, \
                            lagInFrame(last_count, 1, toUInt64(0)) OVER (PARTITION BY node_uuid, bounds ORDER BY bucket_ts) AS prev_count, \
                            lagInFrame(last_counts, 1, CAST([], 'Array(UInt64)')) OVER (PARTITION BY node_uuid, bounds ORDER BY bucket_ts) AS prev_counts \
                     FROM (\
                         SELECT node_uuid, {bucket} AS bucket_ts, bucket_bounds AS bounds, \
                                argMax(toUInt64(ifNull(count, arraySum(bucket_counts))), timestamp) AS last_count, \
                                argMax(bucket_counts, timestamp) AS last_counts \
                         FROM ({union}) WHERE length(bucket_counts) > 0 OR ifNull(count, 0) > 0 GROUP BY node_uuid, bucket_ts, bounds\
                     )\
                 )\
             ) WHERE bucket_ts >= {} GROUP BY bucket_ts, bounds\
         ) ORDER BY bucket_ts SETTINGS use_query_cache = 1",
        req.grid_start_secs
    )
}

fn snapshot_sql(org: &str, value_expr: &str, req: &SeriesRequest) -> String {
    let bucket = bucket_expr("snapshot_time", req.step_secs);
    let mut w = format!(
        "organization_uuid = '{}' AND snapshot_time >= toDateTime64('{}', 3, 'UTC') AND snapshot_time < toDateTime64('{}', 3, 'UTC')",
        escape_clickhouse_string(org),
        clickhouse_time(req.from),
        clickhouse_time(req.to),
    );
    if let Some(("endpoint_uuid", id)) = &req.scope_label {
        w.push_str(&format!(" AND endpoint_uuid = '{}'", escape_clickhouse_string(id)));
    }
    format!(
        "SELECT {bucket} AS bucket_ts, {value_expr} AS value FROM analytics.endpoint_metrics WHERE {w} GROUP BY bucket_ts ORDER BY bucket_ts SETTINGS use_query_cache = 1"
    )
}

fn table_sql(org: &str, table: &str, time_col: &str, value_expr: &str, endpoint_col: Option<&str>, req: &SeriesRequest) -> String {
    let bucket = bucket_expr(time_col, req.step_secs);
    let mut w = format!(
        "organization_uuid = '{}' AND {time_col} >= toDateTime64('{}', 3, 'UTC') AND {time_col} < toDateTime64('{}', 3, 'UTC')",
        escape_clickhouse_string(org),
        clickhouse_time(req.from),
        clickhouse_time(req.to),
    );
    if let (Some(("endpoint_uuid", id)), Some(endpoint_col)) = (&req.scope_label, endpoint_col) {
        w.push_str(&format!(" AND {endpoint_col} = '{}'", escape_clickhouse_string(id)));
    }
    format!(
        "SELECT {bucket} AS bucket_ts, {value_expr} AS value FROM {table} WHERE {w} GROUP BY bucket_ts ORDER BY bucket_ts SETTINGS use_query_cache = 1"
    )
}

// ── ClickHouse row types ──

#[derive(clickhouse::Row, serde::Deserialize)]
struct ScalarRow {
    bucket_ts: u32,
    value: Option<f64>,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct HistRow {
    bucket_ts: u32,
    counts: Vec<u64>,
    bounds: Vec<f64>,
    total: u64,
}

/// Linear-interpolated quantile over a merged histogram. `counts.len()` is
/// `bounds.len() + 1` (the trailing `+inf` overflow bucket).
fn hist_quantile(counts: &[u64], bounds: &[f64], total: u64, q: f64) -> Option<f64> {
    if total == 0 || counts.is_empty() {
        return None;
    }
    let target = q * total as f64;
    let mut cum: u64 = 0;
    for (i, &c) in counts.iter().enumerate() {
        let prev = cum;
        cum = cum.saturating_add(c);
        if cum as f64 >= target {
            let lo = if i == 0 { 0.0 } else { bounds.get(i - 1).copied().unwrap_or(0.0) };
            let hi = bounds.get(i).copied().unwrap_or(lo);
            let frac = if c == 0 { 0.0 } else { (target - prev as f64) / c as f64 };
            return Some(lo + (hi - lo) * frac.clamp(0.0, 1.0));
        }
    }
    bounds.last().copied()
}

/// Fast-telemetry exponential histograms can shift bucket offsets as larger
/// values arrive. Keep incompatible bucket layouts separate in SQL, then render
/// the dominant layout per dashboard bucket instead of merging mismatched arrays.
fn dominant_histogram_rows(rows: &[HistRow]) -> Vec<&HistRow> {
    let mut best_by_bucket: HashMap<u32, &HistRow> = HashMap::new();
    for row in rows {
        if row.total == 0 || row.counts.is_empty() {
            continue;
        }
        let replace = best_by_bucket.get(&row.bucket_ts).map(|existing| row.total > existing.total).unwrap_or(true);
        if replace {
            best_by_bucket.insert(row.bucket_ts, row);
        }
    }
    let mut rows = best_by_bucket.into_values().collect::<Vec<_>>();
    rows.sort_by_key(|row| row.bucket_ts);
    rows
}

/// Place sparse `(bucket_ts, value)` rows into a fixed-length grid array.
/// `fill_zero` distinguishes counters (gap = 0) from gauges/latency (gap = null).
fn fill_grid(rows: &[(u32, f64)], req: &SeriesRequest, fill_zero: bool) -> Vec<Option<f64>> {
    let mut out = vec![if fill_zero { Some(0.0) } else { None }; req.buckets];
    for &(ts, v) in rows {
        let idx = (ts as i64 - req.grid_start_secs) / req.step_secs;
        if idx >= 0 && (idx as usize) < req.buckets {
            out[idx as usize] = Some(v);
        }
    }
    out
}

fn fill_counter_grid(rows: &[(u32, f64)], req: &SeriesRequest) -> Vec<Option<f64>> {
    if rows.is_empty() {
        vec![None; req.buckets]
    } else {
        fill_grid(rows, req, true)
    }
}

// ── Response ──

#[derive(Serialize)]
struct SeriesOut {
    name: String,
    kind: String,
    values: Vec<Option<f64>>,
}

#[derive(Serialize)]
struct SeriesResponse {
    /// First bucket start, epoch ms (UTC).
    t0: i64,
    /// Bucket width, seconds.
    step: i64,
    /// Bucket count.
    n: usize,
    series: Vec<SeriesOut>,
}

/// One cached series response (serialized body + ETag), shared by the L1 cache
/// and Redis. `body` is `Arc`'d so concurrent requests share one allocation.
#[derive(Clone)]
struct CachedSeries {
    body: Arc<String>,
    etag: String,
}

/// In-process L1 + single-flight: `try_get_with` collapses concurrent identical
/// requests into ONE computation (the rest await its result) and serves the
/// cached body for a short window so bursts (every board, every tick) don't each
/// hit Redis/ClickHouse.
static L1: Lazy<moka::future::Cache<String, CachedSeries>> =
    Lazy::new(|| moka::future::Cache::builder().max_capacity(4096).time_to_live(StdDuration::from_secs(10)).build());

/// Per-series-set cache key. Keyed on the aligned grid (step/grid_start/n) so all
/// requests within one step window share, and a new key appears when a bucket opens.
fn cache_key(org: &str, req: &SeriesRequest) -> String {
    let metrics = req.metrics.iter().map(|m| format!("{}|{}", m.name, m.kind)).collect::<Vec<_>>().join(",");
    let scope = req.scope_label.as_ref().map(|(k, v)| format!("{k}={v}")).unwrap_or_default();
    let endpoint_kind = req.endpoint_kind.as_deref().unwrap_or("all");
    format!(
        "an:series:v1:{org}|{scope}|{}|{endpoint_kind}|{}|{}|{}|{metrics}",
        req.traffic_class.cache_key(),
        req.step_secs,
        req.grid_start_secs,
        req.buckets
    )
}

/// Bucket-aware TTL: a window touching "now" has a changing open bucket → short
/// TTL; an older window is effectively immutable → cache longer.
fn cache_ttl(req: &SeriesRequest) -> u64 {
    let now = Utc::now().timestamp();
    let last_bucket_end = req.grid_start_secs + (req.buckets as i64) * req.step_secs;
    if last_bucket_end >= now - req.step_secs {
        (req.step_secs as u64).clamp(5, 30)
    } else {
        300
    }
}

fn compute_etag(body: &str) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    body.hash(&mut h);
    format!("\"{:016x}\"", h.finish())
}

fn child_span(parent: Option<&FastSpan>, name: &'static str) -> Option<FastSpan> {
    parent.map(|span| span.child(name, FastSpanKind::Client))
}

fn set_span_attr(span: &mut Option<FastSpan>, key: &'static str, value: impl Into<FastSpanValue>) {
    if let Some(span) = span {
        span.set_attribute(key, value);
    }
}

fn set_span_ok(span: &mut Option<FastSpan>) {
    if let Some(span) = span {
        span.set_status(FastSpanStatus::Ok);
    }
}

fn set_span_error(span: &mut Option<FastSpan>, error: impl std::fmt::Display) {
    if let Some(span) = span {
        span.set_status(FastSpanStatus::Error { message: error.to_string().into() });
    }
}

/// Single-flight on a moka cache: concurrent calls for the same `key` collapse to
/// ONE `init` execution (the rest await its result). Split out from `cached_series`
/// so the collapse behavior is unit-testable with an injected `init` (no DB).
async fn l1_get_or_compute(
    cache: &moka::future::Cache<String, CachedSeries>,
    key: String,
    init: impl std::future::Future<Output = Result<CachedSeries, String>>,
    parent_span: Option<&FastSpan>,
) -> Result<CachedSeries, actix_web::Error> {
    let mut span = child_span(parent_span, "analytics_series.l1_get_or_compute");
    let result = cache.try_get_with(key, init).await;
    match &result {
        Ok(_) => set_span_ok(&mut span),
        Err(error) => set_span_error(&mut span, error.as_ref()),
    }
    result.map_err(|e: Arc<String>| actix_web::error::ErrorInternalServerError((*e).clone()))
}

/// L1 → Redis → ClickHouse, with single-flight on the L1 layer.
async fn cached_series(
    database: web::Data<EdenDb>,
    org: String,
    request: SeriesRequest,
    parent_span: Option<&FastSpan>,
) -> Result<CachedSeries, actix_web::Error> {
    let mut span = child_span(parent_span, "analytics_series.cached_series");
    set_span_attr(&mut span, "metric_count", request.metrics.len() as i64);
    set_span_attr(&mut span, "bucket_count", request.buckets as i64);
    let key = cache_key(&org, &request);
    let ttl = cache_ttl(&request);
    set_span_attr(&mut span, "ttl_seconds", ttl as i64);

    let db = database.clone();
    let rkey = key.clone();
    let compute_parent = span.as_ref().or(parent_span);
    let init = async move {
        // Redis result cache (stored as "<etag>\n<json>").
        let stored = {
            let mut redis_span = child_span(compute_parent, "analytics_series.redis_get");
            let stored = db.kv_get(&rkey).await;
            match &stored {
                Ok(Some(_)) => {
                    set_span_attr(&mut redis_span, "cache_hit", true);
                    set_span_ok(&mut redis_span);
                }
                Ok(None) => {
                    set_span_attr(&mut redis_span, "cache_hit", false);
                    set_span_ok(&mut redis_span);
                }
                Err(error) => {
                    set_span_attr(&mut redis_span, "cache_error", true);
                    set_span_error(&mut redis_span, error);
                }
            }
            stored
        };
        if let Ok(Some(stored)) = stored {
            if let Some((etag, body)) = stored.split_once('\n') {
                return Ok(CachedSeries { etag: etag.to_string(), body: Arc::new(body.to_string()) });
            }
        }
        let resp = build_response(db.clone(), org, request, compute_parent).await.map_err(|e| e.to_string())?;
        let body = {
            let mut serialize_span = child_span(compute_parent, "analytics_series.serialize_response");
            let body = serde_json::to_string(&resp);
            match &body {
                Ok(_) => set_span_ok(&mut serialize_span),
                Err(error) => set_span_error(&mut serialize_span, error),
            }
            body.map_err(|e| e.to_string())?
        };
        let etag = compute_etag(&body);
        {
            let mut redis_span = child_span(compute_parent, "analytics_series.redis_set");
            set_span_attr(&mut redis_span, "ttl_seconds", ttl as i64);
            let result = db.kv_set_ex(rkey.clone(), format!("{etag}\n{body}"), ttl).await;
            match &result {
                Ok(_) => set_span_ok(&mut redis_span),
                Err(error) => set_span_error(&mut redis_span, error),
            }
        }
        Ok(CachedSeries { etag, body: Arc::new(body) })
    };
    let result = l1_get_or_compute(&L1, key, init, compute_parent).await;
    match &result {
        Ok(_) => set_span_ok(&mut span),
        Err(error) => set_span_error(&mut span, error),
    }
    result
}

/// `GET /api/v1/analytics/series?metrics=eden.request_sent|Counter,...&range=1h&buckets=32[&scope_kind=endpoint&scope_id=...][&traffic_class=external|internal|all][&endpoint_kind=redis|mongo|postgres|llm][&since=<ms>]`
#[with_telemetry]
pub async fn series(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let params = web::Query::<HashMap<String, String>>::from_query(req.query_string())
        .map_err(|e| actix_web::error::ErrorBadRequest(format!("invalid query string: {e}")))?
        .into_inner();
    let request = parse_request(&params)?;

    // RBAC: per-endpoint READ when scoped to an endpoint, else org-wide READ.
    verify_control_perms(&database, &auth, request.scope_endpoint.clone(), ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    let org = auth.org_uuid().uuid().to_string();

    let cached = cached_series(database.clone(), org, request, Some(&span)).await?;

    // Conditional request: unchanged data → 304 (no body).
    if let Some(inm) = req.headers().get("If-None-Match").and_then(|h| h.to_str().ok()) {
        if inm == cached.etag {
            return Ok(HttpResponse::NotModified().insert_header(("ETag", cached.etag.clone())).finish());
        }
    }
    Ok(HttpResponse::Ok()
        .insert_header(("ETag", cached.etag.clone()))
        .insert_header(("Cache-Control", "no-cache"))
        .content_type("application/json")
        .body((*cached.body).clone()))
}

/// Acquire a pooled ClickHouse client, then run the queries.
async fn build_response(
    database: web::Data<EdenDb>,
    org: String,
    request: SeriesRequest,
    parent_span: Option<&FastSpan>,
) -> Result<SeriesResponse, actix_web::Error> {
    let mut span = child_span(parent_span, "analytics_series.build_response");
    let query_parent = span.as_ref().or(parent_span);
    let client = {
        let mut pool_span = child_span(query_parent, "analytics_series.clickhouse_pool_get");
        let client = database.clickhouse_pool().get().await;
        match &client {
            Ok(_) => set_span_ok(&mut pool_span),
            Err(error) => set_span_error(&mut pool_span, error),
        }
        client.map_err(|_| actix_web::error::ErrorServiceUnavailable("analytics backend unavailable"))?
    };
    let result = run_queries(&client, &org, &request, query_parent).await;
    match &result {
        Ok(_) => set_span_ok(&mut span),
        Err(error) => set_span_error(&mut span, error),
    }
    result
}

/// Run the per-metric queries and assemble the columnar response (no caching).
/// Takes the analytics client directly so integration tests can drive it without the service pool.
async fn run_queries(
    client: &AnalyticsSeriesClient,
    org: &str,
    request: &SeriesRequest,
    parent_span: Option<&FastSpan>,
) -> Result<SeriesResponse, actix_web::Error> {
    let mut span = child_span(parent_span, "analytics_series.run_queries");
    set_span_attr(&mut span, "metric_count", request.metrics.len() as i64);
    let query_parent = span.as_ref().or(parent_span);
    let mut series = Vec::with_capacity(request.metrics.len());
    for m in &request.metrics {
        let mut out = Vec::new();
        match &m.source {
            Source::Snapshot { value_expr } => {
                let sql = snapshot_sql(org, value_expr, request);
                let mut query_span = child_span(query_parent, "analytics_series.clickhouse_query");
                set_span_attr(&mut query_span, "metric_name", m.name.clone());
                set_span_attr(&mut query_span, "metric_kind", m.kind.clone());
                set_span_attr(&mut query_span, "query_source", "snapshot");
                let rows_result: Result<Vec<ScalarRow>, _> = client.query(&sql).fetch_all().await;
                match &rows_result {
                    Ok(rows) => {
                        set_span_attr(&mut query_span, "row_count", rows.len() as i64);
                        set_span_ok(&mut query_span);
                    }
                    Err(error) => set_span_error(&mut query_span, error),
                }
                let rows = rows_result.map_err(query_err)?;
                let pairs: Vec<(u32, f64)> = rows.into_iter().filter_map(|r| r.value.map(|value| (r.bucket_ts, value))).collect();
                out.push(SeriesOut {
                    name: m.name.clone(),
                    kind: m.kind.clone(),
                    values: fill_grid(&pairs, request, false),
                });
            }
            Source::Table { table, time_col, value_expr, endpoint_col } => {
                let sql = table_sql(org, table, time_col, value_expr, *endpoint_col, request);
                let mut query_span = child_span(query_parent, "analytics_series.clickhouse_query");
                set_span_attr(&mut query_span, "metric_name", m.name.clone());
                set_span_attr(&mut query_span, "metric_kind", m.kind.clone());
                set_span_attr(&mut query_span, "query_source", "table");
                set_span_attr(&mut query_span, "query_table", *table);
                let rows_result: Result<Vec<ScalarRow>, _> = client.query(&sql).fetch_all().await;
                match &rows_result {
                    Ok(rows) => {
                        set_span_attr(&mut query_span, "row_count", rows.len() as i64);
                        set_span_ok(&mut query_span);
                    }
                    Err(error) => set_span_error(&mut query_span, error),
                }
                let rows = rows_result.map_err(query_err)?;
                let pairs: Vec<(u32, f64)> = rows.into_iter().filter_map(|r| r.value.map(|value| (r.bucket_ts, value))).collect();
                out.push(SeriesOut {
                    name: m.name.clone(),
                    kind: m.kind.clone(),
                    values: fill_grid(&pairs, request, false),
                });
            }
            Source::Generic { metric_name, strategy } => match strategy {
                Strategy::Counter => {
                    let sql = counter_sql(org, metric_name, request);
                    let mut query_span = child_span(query_parent, "analytics_series.clickhouse_query");
                    set_span_attr(&mut query_span, "metric_name", m.name.clone());
                    set_span_attr(&mut query_span, "metric_kind", m.kind.clone());
                    set_span_attr(&mut query_span, "query_source", "generic");
                    set_span_attr(&mut query_span, "query_strategy", "counter");
                    let rows_result: Result<Vec<ScalarRow>, _> = client.query(&sql).fetch_all().await;
                    match &rows_result {
                        Ok(rows) => {
                            set_span_attr(&mut query_span, "row_count", rows.len() as i64);
                            set_span_ok(&mut query_span);
                        }
                        Err(error) => set_span_error(&mut query_span, error),
                    }
                    let rows = rows_result.map_err(query_err)?;
                    let pairs: Vec<(u32, f64)> = rows.into_iter().filter_map(|r| r.value.map(|value| (r.bucket_ts, value))).collect();
                    out.push(SeriesOut {
                        name: m.name.clone(),
                        kind: m.kind.clone(),
                        values: fill_counter_grid(&pairs, request),
                    });
                }
                Strategy::UpDownCounter => {
                    let sql = up_down_counter_sql(org, metric_name, request);
                    let mut query_span = child_span(query_parent, "analytics_series.clickhouse_query");
                    set_span_attr(&mut query_span, "metric_name", m.name.clone());
                    set_span_attr(&mut query_span, "metric_kind", m.kind.clone());
                    set_span_attr(&mut query_span, "query_source", "generic");
                    set_span_attr(&mut query_span, "query_strategy", "up_down_counter");
                    let rows_result: Result<Vec<ScalarRow>, _> = client.query(&sql).fetch_all().await;
                    match &rows_result {
                        Ok(rows) => {
                            set_span_attr(&mut query_span, "row_count", rows.len() as i64);
                            set_span_ok(&mut query_span);
                        }
                        Err(error) => set_span_error(&mut query_span, error),
                    }
                    let rows = rows_result.map_err(query_err)?;
                    let pairs: Vec<(u32, f64)> = rows.into_iter().filter_map(|r| r.value.map(|value| (r.bucket_ts, value))).collect();
                    out.push(SeriesOut {
                        name: m.name.clone(),
                        kind: m.kind.clone(),
                        values: fill_grid(&pairs, request, false),
                    });
                }
                Strategy::Gauge => {
                    let sql = gauge_sql(org, metric_name, request);
                    let mut query_span = child_span(query_parent, "analytics_series.clickhouse_query");
                    set_span_attr(&mut query_span, "metric_name", m.name.clone());
                    set_span_attr(&mut query_span, "metric_kind", m.kind.clone());
                    set_span_attr(&mut query_span, "query_source", "generic");
                    set_span_attr(&mut query_span, "query_strategy", "gauge");
                    let rows_result: Result<Vec<ScalarRow>, _> = client.query(&sql).fetch_all().await;
                    match &rows_result {
                        Ok(rows) => {
                            set_span_attr(&mut query_span, "row_count", rows.len() as i64);
                            set_span_ok(&mut query_span);
                        }
                        Err(error) => set_span_error(&mut query_span, error),
                    }
                    let rows = rows_result.map_err(query_err)?;
                    let pairs: Vec<(u32, f64)> = rows.into_iter().filter_map(|r| r.value.map(|value| (r.bucket_ts, value))).collect();
                    out.push(SeriesOut {
                        name: m.name.clone(),
                        kind: m.kind.clone(),
                        values: fill_grid(&pairs, request, false),
                    });
                }
                Strategy::Histogram => {
                    let sql = histogram_sql(org, metric_name, request);
                    let mut query_span = child_span(query_parent, "analytics_series.clickhouse_query");
                    set_span_attr(&mut query_span, "metric_name", m.name.clone());
                    set_span_attr(&mut query_span, "metric_kind", m.kind.clone());
                    set_span_attr(&mut query_span, "query_source", "generic");
                    set_span_attr(&mut query_span, "query_strategy", "histogram");
                    let rows_result: Result<Vec<HistRow>, _> = client.query(&sql).fetch_all().await;
                    match &rows_result {
                        Ok(rows) => {
                            set_span_attr(&mut query_span, "row_count", rows.len() as i64);
                            set_span_ok(&mut query_span);
                        }
                        Err(error) => set_span_error(&mut query_span, error),
                    }
                    let rows = rows_result.map_err(query_err)?;
                    let dominant_rows = dominant_histogram_rows(&rows);
                    // Emit p50/p95/p99 as three suffixed series.
                    for (q, suffix) in [(0.50, ":p50"), (0.95, ":p95"), (0.99, ":p99")] {
                        let pairs: Vec<(u32, f64)> = dominant_rows
                            .iter()
                            .filter_map(|r| hist_quantile(&r.counts, &r.bounds, r.total, q).map(|v| (r.bucket_ts, v)))
                            .collect();
                        out.push(SeriesOut {
                            name: format!("{}{suffix}", m.name),
                            kind: m.kind.clone(),
                            values: fill_grid(&pairs, request, false),
                        });
                    }
                }
            },
        }
        series.extend(out);
    }

    let response = SeriesResponse {
        t0: request.grid_start_secs * 1000,
        step: request.step_secs,
        n: request.buckets,
        series,
    };
    set_span_attr(&mut span, "output_series_count", response.series.len() as i64);
    set_span_ok(&mut span);
    Ok(response)
}

fn query_err(err: impl std::fmt::Display) -> actix_web::Error {
    log::error!("analytics series query failed: {err}");
    actix_web::error::ErrorInternalServerError("failed to query analytics series")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(step: i64, from_secs: i64) -> SeriesRequest {
        SeriesRequest {
            metrics: vec![],
            from: DateTime::from_timestamp(from_secs, 0).unwrap(),
            to: DateTime::from_timestamp(from_secs + step * 4, 0).unwrap(),
            buckets: 4,
            step_secs: step,
            grid_start_secs: (from_secs / step) * step,
            scope_label: None,
            scope_endpoint: None,
            traffic_class: TrafficClassFilter::All,
            endpoint_kind: None,
        }
    }

    #[test]
    fn fill_grid_places_and_fills() {
        let r = req(60, 0);
        let counter = fill_grid(&[(0, 5.0), (120, 7.0)], &r, true);
        assert_eq!(counter, vec![Some(5.0), Some(0.0), Some(7.0), Some(0.0)]);
        let gauge = fill_grid(&[(60, 3.0)], &r, false);
        assert_eq!(gauge, vec![None, Some(3.0), None, None]);
    }

    #[test]
    fn fill_counter_grid_distinguishes_missing_from_zero() {
        let r = req(60, 0);
        assert_eq!(fill_counter_grid(&[], &r), vec![None, None, None, None]);
        assert_eq!(fill_counter_grid(&[(120, 7.0)], &r), vec![Some(0.0), Some(0.0), Some(7.0), Some(0.0)]);
    }

    #[test]
    fn resolve_picks_snapshot_for_analytics_endpoint() {
        match resolve("eden.analytics.endpoint.ops_per_sec", "Gauge").source {
            Source::Snapshot { value_expr } => assert_eq!(value_expr, "avg(ops_per_sec)"),
            _ => panic!("expected snapshot"),
        }
        match resolve("eden.request_sent", "Counter").source {
            Source::Generic { strategy, .. } => assert!(strategy == Strategy::Counter),
            _ => panic!("expected generic"),
        }
        match resolve("eden.active_requests", "UpDownCounter").source {
            Source::Generic { strategy, .. } => assert!(strategy == Strategy::UpDownCounter),
            _ => panic!("expected generic"),
        }
    }

    #[test]
    fn quantile_interpolates() {
        // counts over bounds [10,20,30] + overflow; total 10; p50 → median.
        let q = hist_quantile(&[2, 4, 4, 0], &[10.0, 20.0, 30.0], 10, 0.5).unwrap();
        assert!(q > 10.0 && q < 30.0, "q={q}");
    }

    #[test]
    fn counter_sql_uses_window_diff_not_naive_sum() {
        let r = req(60, 0);
        let sql = counter_sql("org", "eden.request_sent", &r);
        assert!(sql.contains("lagInFrame"));
        assert!(sql.contains("lagInFrame(last_val, 1, 0.0)"));
        assert!(sql.contains("argMax(ifNull(value, 0.0), timestamp)"));
        assert!(sql.contains("toNullable(sum(delta)) AS value"));
        assert!(sql.contains("WHERE bucket_ts >="));
        assert!(sql.contains("use_query_cache = 1"));
        assert!(!sql.contains("sum(value)"));
    }

    #[test]
    fn up_down_counter_sql_sums_latest_label_series() {
        let r = req(60, 0);
        let sql = up_down_counter_sql("org", "eden.active_requests", &r);
        assert!(sql.contains("cityHash64(toString(labels)) AS labels_hash"));
        assert!(sql.contains("argMax(ifNull(value, 0.0), timestamp) AS last_val"));
        assert!(sql.contains("toNullable(sum(last_val)) AS value"));
        assert!(sql.contains("query_cache = 1"));
        assert!(!sql.contains("avg(value)"));
    }

    #[test]
    fn generic_where_is_org_scoped() {
        let r = req(60, 0);
        let w = generic_where("authorized-org", "eden.request_sent", &r);
        assert!(w.contains("organization_uuid = 'authorized-org'"));
        assert!(w.contains("metric_name IN ('eden.request_sent', 'eden_request_count'"));
    }

    #[test]
    fn traffic_class_filter_shapes_generic_where() {
        let mut r = req(60, 0);
        r.traffic_class = TrafficClassFilter::External;
        let external = generic_where("org", "eden.request_sent", &r);
        assert!(external.contains("labels['traffic_class'] = 'external'"));

        r.traffic_class = TrafficClassFilter::Internal;
        let internal = generic_where("org", "eden.request_sent", &r);
        assert!(internal.contains("labels['traffic_class'] = 'internal'"));

        r.traffic_class = TrafficClassFilter::All;
        let all = generic_where("org", "eden.request_sent", &r);
        assert!(!all.contains("labels['traffic_class']"));
    }

    #[test]
    fn endpoint_kind_filter_shapes_generic_where() {
        let mut r = req(60, 0);
        r.endpoint_kind = Some("redis".to_string());
        let where_clause = generic_where("org", "gateway.requests_total", &r);

        assert!(where_clause.contains("labels['endpoint_kind'] = 'redis'"));
    }

    #[test]
    fn metric_name_filter_uses_exact_production_name() {
        assert_eq!(
            metric_name_filter("gateway.request_duration_microseconds"),
            "metric_name IN ('gateway.request_duration_microseconds', 'gateway_request_duration_microseconds')"
        );
    }

    #[test]
    fn metric_name_candidates_include_fast_telemetry_aliases() {
        assert_eq!(metric_name_candidates("eden.request_sent"), vec!["eden.request_sent", "eden_request_count"]);
        assert_eq!(
            metric_name_candidates("eden.endpoint.endpoint_duration"),
            vec!["eden.endpoint.endpoint_duration", "eden.endpoint_endpoint_duration"]
        );
        assert_eq!(
            metric_name_candidates("eden.llm.gateway.requests"),
            vec!["eden.llm.gateway.requests", "eden_llm_gateway_requests"]
        );
        assert_eq!(
            metric_name_candidates("workload.workload_ratio"),
            vec!["workload.workload_ratio", "workload_workload_ratio"]
        );
    }

    #[test]
    fn metric_name_candidates_include_gateway_redis_legacy_aliases() {
        assert_eq!(
            metric_name_candidates("gateway.redis.command_end_to_end_microseconds"),
            vec![
                "gateway.redis.command_end_to_end_microseconds",
                "gateway_redis_command_end_to_end_microseconds",
                "gateway.command_end_to_end_microseconds",
                "gateway_command_end_to_end_microseconds",
            ]
        );
        assert_eq!(
            metric_name_candidates("gateway.commands_total"),
            vec![
                "gateway.commands_total",
                "gateway_commands_total",
                "gateway.redis.commands_total",
                "gateway_redis_commands_total",
            ]
        );
    }

    #[test]
    fn resolve_picks_table_sources_for_analytics_rollups() {
        match resolve("eden.analytics.command.requests", "Gauge").source {
            Source::Table { table, time_col, endpoint_col, .. } => {
                assert_eq!(table, "analytics.command_rollups");
                assert_eq!(time_col, "window_start");
                assert_eq!(endpoint_col, Some("endpoint_uuid"));
            }
            _ => panic!("expected command rollup table source"),
        }
        match resolve("eden.analytics.api.server_errors", "Gauge").source {
            Source::Table { table, time_col, endpoint_col, .. } => {
                assert_eq!(table, "analytics.api_usage_history");
                assert_eq!(time_col, "request_time");
                assert_eq!(endpoint_col, Some("endpoint_uuid"));
            }
            _ => panic!("expected API usage table source"),
        }
        match resolve("eden.analytics.snapshot.bytes_written_total", "Gauge").source {
            Source::Table { table, endpoint_col, .. } => {
                assert_eq!(table, "analytics.infrastructure_snapshots");
                assert_eq!(endpoint_col, Some("source_endpoint_uuid"));
            }
            _ => panic!("expected infrastructure snapshot table source"),
        }
    }

    // ── Group A: deterministic input/validation/cache unit tests (no Docker) ──

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn err_status(r: Result<SeriesRequest, actix_web::Error>) -> u16 {
        r.err().expect("expected an error").as_response_error().status_code().as_u16()
    }

    fn err_msg(r: Result<SeriesRequest, actix_web::Error>) -> String {
        r.err().expect("expected an error").to_string()
    }

    // Fixed, highly-aligned window: 2021-01-01T00:00:00Z = 1609459200 (divisible by
    // 15/60/300/3600); +1h = 1609462800.
    const F: &str = "2021-01-01T00:00:00Z";
    const T: &str = "2021-01-01T01:00:00Z";
    const F_TS: i64 = 1_609_459_200;
    const T_TS: i64 = 1_609_462_800;

    /// Build a `SeriesRequest` directly (for cache-layer tests that don't need a DB).
    fn sr(metrics: &[(&str, &str)], scope: Option<(&'static str, String)>, step: i64, grid: i64, buckets: usize) -> SeriesRequest {
        SeriesRequest {
            metrics: metrics.iter().map(|(n, k)| resolve(n, k)).collect(),
            from: DateTime::from_timestamp(grid, 0).unwrap(),
            to: DateTime::from_timestamp(grid + step * buckets as i64, 0).unwrap(),
            buckets,
            step_secs: step,
            grid_start_secs: grid,
            scope_label: scope,
            scope_endpoint: None,
            traffic_class: TrafficClassFilter::All,
            endpoint_kind: None,
        }
    }

    #[test]
    fn parse_request_resolves_metrics_and_grid() {
        let r = parse_request(&params(&[
            ("metrics", "eden.request_sent|Counter,foo,eden.analytics.endpoint.ops_per_sec|Gauge"),
            ("from", F),
            ("to", T),
            ("buckets", "12"),
        ]))
        .expect("ok");
        assert_eq!(r.metrics.len(), 3);
        match &r.metrics[0].source {
            Source::Generic { strategy, metric_name } => {
                assert!(*strategy == Strategy::Counter);
                assert_eq!(metric_name, "eden.request_sent");
            }
            _ => panic!("expected generic counter"),
        }
        assert_eq!(r.metrics[0].kind, "Counter");
        // No `|kind` → defaults to Gauge.
        match &r.metrics[1].source {
            Source::Generic { strategy, .. } => assert!(*strategy == Strategy::Gauge),
            _ => panic!("expected generic gauge"),
        }
        assert_eq!(r.metrics[1].kind, "Gauge");
        // endpoint.* leaf resolves to the pre-aggregated snapshot source.
        match &r.metrics[2].source {
            Source::Snapshot { .. } => {}
            _ => panic!("expected snapshot"),
        }
        // range = 3600, buckets = 12 → step 300; window is step-aligned.
        assert_eq!(r.step_secs, 300);
        assert_eq!(r.buckets, 12);
        assert_eq!(r.grid_start_secs, F_TS);
    }

    #[test]
    fn parse_request_rejects_bad_input() {
        // metrics missing / empty.
        assert_eq!(err_status(parse_request(&params(&[]))), 400);
        assert!(err_msg(parse_request(&params(&[]))).contains("metrics is required"));
        assert_eq!(err_status(parse_request(&params(&[("metrics", ",, ")]))), 400);
        assert!(err_msg(parse_request(&params(&[("metrics", ",, ")]))).contains("no valid metrics"));
        // metric-name injection guard.
        for bad in ["bad name", "a;b", "a'b", "evil(x)", "a`b"] {
            assert_eq!(err_status(parse_request(&params(&[("metrics", bad)]))), 400, "should reject {bad:?}");
        }
        // > MAX_METRICS.
        let many = (0..(MAX_METRICS + 1)).map(|i| format!("m{i}")).collect::<Vec<_>>().join(",");
        assert_eq!(err_status(parse_request(&params(&[("metrics", many.as_str()), ("from", F), ("to", T)]))), 400);
        // from >= to.
        let inv = parse_request(&params(&[("metrics", "foo"), ("from", T), ("to", F)]));
        assert_eq!(err_status(parse_request(&params(&[("metrics", "foo"), ("from", T), ("to", F)]))), 400);
        assert!(err_msg(inv).contains("from must be before to"));
        // range too large.
        assert_eq!(
            err_status(parse_request(&params(&[
                ("metrics", "foo"),
                ("from", "2000-01-01T00:00:00Z"),
                ("to", "2030-01-01T00:00:00Z")
            ]))),
            400
        );
    }

    #[test]
    fn parse_request_clamps_buckets_and_derives_step() {
        // buckets clamped to MAX_BUCKETS (240) → step = ceil(3600/240) = 15.
        let big = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("buckets", "500")])).expect("ok");
        assert_eq!(big.buckets, 240);
        assert_eq!(big.step_secs, 15);
        // buckets clamped up to 1 → step = ceil(3600/1) = 3600.
        let zero = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("buckets", "0")])).expect("ok");
        assert_eq!(zero.buckets, 1);
        assert_eq!(zero.step_secs, 3600);
        // unparseable buckets → default 32 → step = ceil(3600/32) = 113.
        let bad = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("buckets", "abc")])).expect("ok");
        assert_eq!(bad.buckets, 32);
        assert_eq!(bad.step_secs, 113);
        // from/to takes precedence over `range` for the window size.
        let p = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("range", "9999h"), ("buckets", "12")])).expect("ok");
        assert_eq!(p.step_secs, 300);
    }

    #[test]
    fn parse_request_since_cursor() {
        // since 1500s into the window → grid advances to that aligned bucket, n shrinks.
        let since = ((F_TS + 1500) * 1000).to_string();
        let s = parse_request(&params(&[
            ("metrics", "foo"),
            ("from", F),
            ("to", T),
            ("buckets", "12"),
            ("since", since.as_str()),
        ]))
        .expect("ok");
        assert_eq!(s.step_secs, 300);
        assert_eq!(s.grid_start_secs, F_TS + 1500);
        assert_eq!(s.buckets, 7); // (3600-1500)/300
        // since before the window → full grid.
        let before = ((F_TS - 1000) * 1000).to_string();
        let b = parse_request(&params(&[
            ("metrics", "foo"),
            ("from", F),
            ("to", T),
            ("buckets", "12"),
            ("since", before.as_str()),
        ]))
        .expect("ok");
        assert_eq!(b.buckets, 12);
        assert_eq!(b.grid_start_secs, F_TS);
        // since at/after `to` → never 0 / never panics.
        let after = ((T_TS + 100) * 1000).to_string();
        let a = parse_request(&params(&[
            ("metrics", "foo"),
            ("from", F),
            ("to", T),
            ("buckets", "12"),
            ("since", after.as_str()),
        ]))
        .expect("ok");
        assert!(a.buckets >= 1 && a.buckets <= 12, "n={}", a.buckets);
    }

    #[test]
    fn parse_request_scope() {
        // interlay/migration/api → matching label, no RBAC endpoint.
        let il = parse_request(&params(&[
            ("metrics", "foo"),
            ("from", F),
            ("to", T),
            ("scope_kind", "interlay"),
            ("scope_id", "il-123"),
        ]))
        .expect("ok");
        assert_eq!(il.scope_label, Some(("interlay_uuid", "il-123".to_string())));
        assert!(il.scope_endpoint.is_none());
        let mg = parse_request(&params(&[
            ("metrics", "foo"),
            ("from", F),
            ("to", T),
            ("scope_kind", "migration"),
            ("scope_id", "m1"),
        ]))
        .expect("ok");
        assert_eq!(mg.scope_label, Some(("migration_uuid", "m1".to_string())));
        let api = parse_request(&params(&[
            ("metrics", "foo"),
            ("from", F),
            ("to", T),
            ("scope_kind", "api"),
            ("scope_id", "a1"),
        ]))
        .expect("ok");
        assert_eq!(api.scope_label, Some(("api_id", "a1".to_string())));
        // endpoint → endpoint_uuid label AND a parsed RBAC endpoint uuid.
        let ep = parse_request(&params(&[
            ("metrics", "foo"),
            ("from", F),
            ("to", T),
            ("scope_kind", "endpoint"),
            ("scope_id", "8ef8ebf1-a3e0-4a23-86c8-0afd711eecbd"),
        ]))
        .expect("ok");
        assert_eq!(ep.scope_label.as_ref().map(|(l, _)| *l), Some("endpoint_uuid"));
        assert!(ep.scope_endpoint.is_some());
        // function → endpoint_uuid label but NOT an RBAC endpoint.
        let fnsc = parse_request(&params(&[
            ("metrics", "foo"),
            ("from", F),
            ("to", T),
            ("scope_kind", "function"),
            ("scope_id", "fn-1"),
        ]))
        .expect("ok");
        assert_eq!(fnsc.scope_label.as_ref().map(|(l, _)| *l), Some("endpoint_uuid"));
        assert!(fnsc.scope_endpoint.is_none());
        // unknown scope_kind → 400.
        assert_eq!(
            err_status(parse_request(&params(&[
                ("metrics", "foo"),
                ("from", F),
                ("to", T),
                ("scope_kind", "bogus"),
                ("scope_id", "x")
            ]))),
            400
        );
        // scope_kind without scope_id → no filter.
        let nf = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("scope_kind", "endpoint")])).expect("ok");
        assert!(nf.scope_label.is_none() && nf.scope_endpoint.is_none());
        // injection in scope_id → 400.
        assert_eq!(
            err_status(parse_request(&params(&[
                ("metrics", "foo"),
                ("from", F),
                ("to", T),
                ("scope_kind", "interlay"),
                ("scope_id", "bad'id")
            ]))),
            400
        );
    }

    #[test]
    fn parse_request_traffic_class() {
        let defaulted = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T)])).expect("ok");
        assert!(defaulted.traffic_class == TrafficClassFilter::All);
        let external = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("traffic_class", "external")])).expect("ok");
        assert!(external.traffic_class == TrafficClassFilter::External);
        let internal = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("traffic_class", "internal")])).expect("ok");
        assert!(internal.traffic_class == TrafficClassFilter::Internal);
        assert_eq!(
            err_status(parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("traffic_class", "unknown")]))),
            400
        );
    }

    #[test]
    fn parse_request_endpoint_kind_filter() {
        let defaulted = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T)])).expect("ok");
        assert_eq!(defaulted.endpoint_kind, None);

        let redis = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("endpoint_kind", "Redis")])).expect("ok");
        assert_eq!(redis.endpoint_kind.as_deref(), Some("redis"));

        let all = parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("endpoint_kind", "all")])).expect("ok");
        assert_eq!(all.endpoint_kind, None);

        assert_eq!(
            err_status(parse_request(&params(&[("metrics", "foo"), ("from", F), ("to", T), ("endpoint_kind", "bad'kind")]))),
            400
        );
    }

    #[test]
    fn cache_key_isolates_organization_scope_and_window() {
        let base = sr(&[("m", "Gauge")], None, 60, 0, 4);
        // Identical logical request → identical key.
        assert_eq!(cache_key("orgA", &base), cache_key("orgA", &sr(&[("m", "Gauge")], None, 60, 0, 4)));
        // Any of org / metric / scope / step / grid / buckets differing → different key.
        assert_ne!(cache_key("orgA", &base), cache_key("orgB", &base), "organization UUID must not collide");
        assert_ne!(cache_key("orgA", &base), cache_key("orgA", &sr(&[("m2", "Gauge")], None, 60, 0, 4)));
        assert_ne!(
            cache_key("orgA", &base),
            cache_key("orgA", &sr(&[("m", "Gauge")], Some(("endpoint_uuid", "ep".into())), 60, 0, 4))
        );
        let mut internal = sr(&[("m", "Gauge")], None, 60, 0, 4);
        internal.traffic_class = TrafficClassFilter::Internal;
        assert_ne!(cache_key("orgA", &base), cache_key("orgA", &internal));
        let mut redis = sr(&[("m", "Gauge")], None, 60, 0, 4);
        redis.endpoint_kind = Some("redis".to_string());
        assert_ne!(cache_key("orgA", &base), cache_key("orgA", &redis));
        assert_ne!(
            cache_key("orgA", &sr(&[("m", "Counter")], None, 60, 0, 4)),
            cache_key("orgA", &sr(&[("m", "Gauge")], None, 60, 0, 4)),
            "same metric name with different aggregation kind must not collide"
        );
        assert_ne!(cache_key("orgA", &base), cache_key("orgA", &sr(&[("m", "Gauge")], None, 120, 0, 4)));
        assert_ne!(cache_key("orgA", &base), cache_key("orgA", &sr(&[("m", "Gauge")], None, 60, 60, 4)));
        assert_ne!(cache_key("orgA", &base), cache_key("orgA", &sr(&[("m", "Gauge")], None, 60, 0, 8)));
        // Same aligned grid but a different `to` within it → SAME key (share the window).
        let mut other = sr(&[("m", "Gauge")], None, 60, 0, 4);
        other.to = DateTime::from_timestamp(999_999, 0).unwrap();
        assert_eq!(cache_key("orgA", &base), cache_key("orgA", &other));
    }

    #[test]
    fn cache_ttl_open_window_is_short_closed_is_long() {
        let now = Utc::now().timestamp();
        // Window whose last bucket touches ~now → short TTL clamped to step.
        let grid = (now / 10) * 10;
        assert_eq!(cache_ttl(&sr(&[("m", "Gauge")], None, 10, grid, 10)), 10);
        // Window far in the past → immutable → long TTL.
        assert_eq!(cache_ttl(&sr(&[("m", "Gauge")], None, 60, 1_000_000, 10)), 300);
    }

    #[test]
    fn etag_is_stable_and_quoted() {
        assert_eq!(compute_etag("abc"), compute_etag("abc"));
        assert_ne!(compute_etag("abc"), compute_etag("abd"));
        let e = compute_etag("hello");
        assert!(e.starts_with('"') && e.ends_with('"'));
        assert_eq!(e.len(), 18); // '"' + 16 hex + '"'
    }

    #[test]
    fn hist_quantile_edges() {
        assert!(hist_quantile(&[0, 0], &[10.0], 0, 0.5).is_none(), "total 0 → None");
        assert!(hist_quantile(&[], &[], 5, 0.5).is_none(), "empty counts → None");
        // All mass in the trailing +inf overflow bucket → last bound.
        let ov = hist_quantile(&[0, 0, 5], &[10.0, 20.0], 5, 0.99).unwrap();
        assert!((ov - 20.0).abs() < 0.01, "ov={ov}");
        assert_eq!(hist_quantile(&[2, 3], &[10.0], 5, 0.0), Some(0.0));
        let hi = hist_quantile(&[2, 3, 0], &[10.0, 20.0], 5, 1.0).unwrap();
        assert!((hi - 20.0).abs() < 0.01, "hi={hi}");
        assert_eq!(hist_quantile(&[5], &[], 5, 0.5), Some(0.0));
        // Malformed (counts.len() != bounds.len()+1) must not panic.
        let _ = hist_quantile(&[1, 2, 3, 4, 5], &[10.0], 15, 0.5);
    }

    #[test]
    fn dominant_histogram_rows_chooses_one_layout_per_bucket() {
        let small_layout = HistRow {
            bucket_ts: 60,
            counts: vec![1],
            bounds: vec![1024.0],
            total: 1,
        };
        let real_layout = HistRow {
            bucket_ts: 60,
            counts: vec![1, 2, 7],
            bounds: vec![10.0, 100.0, 1000.0],
            total: 10,
        };
        let next_bucket = HistRow {
            bucket_ts: 120,
            counts: vec![3, 2],
            bounds: vec![50.0, 100.0],
            total: 5,
        };
        let zero_only = HistRow { bucket_ts: 180, counts: vec![9], bounds: vec![], total: 9 };

        let rows = vec![small_layout, real_layout, next_bucket, zero_only];
        let dominant = dominant_histogram_rows(&rows);

        assert_eq!(dominant.len(), 3);
        assert_eq!(dominant[0].bucket_ts, 60);
        assert_eq!(dominant[0].bounds, vec![10.0, 100.0, 1000.0]);
        assert_eq!(dominant[1].bucket_ts, 120);
        assert_eq!(dominant[2].bucket_ts, 180);
        assert_eq!(dominant[2].bounds, Vec::<f64>::new());
    }

    #[test]
    fn fill_grid_edges() {
        let r = req(60, 0); // buckets 4, step 60, grid 0
        assert_eq!(fill_grid(&[], &r, true), vec![Some(0.0); 4]);
        assert_eq!(fill_grid(&[], &r, false), vec![None; 4]);
        // bucket_ts beyond the grid is dropped (no panic, no stray value).
        assert_eq!(fill_grid(&[(1_000_000, 9.0)], &r, false), vec![None; 4]);
        // bucket_ts before grid_start (negative index) is dropped.
        let r2 = req(60, 600); // grid_start 600
        assert_eq!(fill_grid(&[(0, 5.0)], &r2, true), vec![Some(0.0); 4]);
    }

    #[test]
    fn sql_builders_shape_and_escaping() {
        let r = req(60, 0);
        // Gauge: regression guard for the ILLEGAL_AGGREGATION fix — avg, no isNotNull.
        let g = gauge_sql("org", "m", &r);
        assert!(g.contains("avg(value)"));
        assert!(!g.contains("isNotNull"), "gauge_sql must not reintroduce the aliased-column WHERE");
        assert!(g.contains("use_query_cache = 1"));
        // Histogram merge + zero-count guard. Histograms are cumulative
        // snapshots, so bucket values must be diffed per node before merging.
        let h = histogram_sql("org", "m", &r);
        assert!(h.contains("argMax(bucket_counts, timestamp) AS last_counts"));
        assert!(h.contains("lagInFrame(last_counts"));
        assert!(h.contains("arrayMap((current, previous)"));
        assert!(h.contains("toUInt64(current - previous)"));
        assert!(h.contains("toUInt64(last_count - prev_count)"));
        assert!(h.contains("sumForEach(delta_counts) AS bucket_deltas"));
        assert!(h.contains("length(bucket_counts) > 0"));
        assert!(h.contains("ifNull(count, 0) > 0"));
        assert!(h.contains("zero_total"));
        assert!(h.contains("GROUP BY bucket_ts, bounds"));
        assert!(!h.contains("sumForEach(bucket_counts)"));
        assert!(h.contains("use_query_cache = 1"));
        // Counter uses the CH native cache too.
        assert!(counter_sql("org", "m", &r).contains("use_query_cache = 1"));
        // Snapshot: organization/time filter, endpoint filter only when scoped.
        let s = snapshot_sql("org", "avg(ops_per_sec)", &r);
        assert!(s.contains("organization_uuid = 'org'"));
        assert!(s.contains("snapshot_time >="));
        assert!(!s.contains("endpoint_uuid ="));
        assert!(s.contains("use_query_cache = 1"));
        let mut rs = req(60, 0);
        rs.scope_label = Some(("endpoint_uuid", "ep1".to_string()));
        assert!(snapshot_sql("org", "avg(ops_per_sec)", &rs).contains("endpoint_uuid = 'ep1'"));
        // Injection: a single quote in org / scope_id is backslash-escaped (no breakout).
        assert!(gauge_sql("o'rg", "m", &r).contains("o\\'rg"));
        let mut rsc = req(60, 0);
        rsc.scope_label = Some(("endpoint_uuid", "e'1".to_string()));
        assert!(gauge_sql("org", "m", &rsc).contains("e\\'1"));
    }

    // ── Group C: single-flight / cache-hit guarantee (no Docker) ──

    fn fresh_l1() -> moka::future::Cache<String, CachedSeries> {
        moka::future::Cache::builder().max_capacity(64).build()
    }

    #[tokio::test]
    async fn l1_single_flight_collapses_concurrent_calls() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = fresh_l1();
        let calls = Arc::new(AtomicUsize::new(0));
        // N concurrent calls for the SAME key must run `init` exactly once.
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let calls = calls.clone();
            set.spawn(async move {
                // Map to Send types — actix_web::Error is !Send and can't cross the task boundary.
                l1_get_or_compute(
                    &cache,
                    "same".to_string(),
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        // Hold the in-flight slot so the other callers pile up behind it.
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        Ok(CachedSeries {
                            etag: "\"shared\"".to_string(),
                            body: Arc::new("body".to_string()),
                        })
                    },
                    None,
                )
                .await
                .map(|c| c.etag)
                .map_err(|e| e.to_string())
            });
        }
        let mut etags = Vec::new();
        while let Some(res) = set.join_next().await {
            etags.push(res.expect("join").expect("compute"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "single-flight must collapse to one compute");
        assert_eq!(etags.len(), 16);
        assert!(etags.iter().all(|e| e == "\"shared\""), "all callers share the one result");
    }

    #[tokio::test]
    async fn l1_distinct_keys_do_not_collapse() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = fresh_l1();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut set = tokio::task::JoinSet::new();
        for key in ["a", "b"] {
            let cache = cache.clone();
            let calls = calls.clone();
            set.spawn(async move {
                l1_get_or_compute(
                    &cache,
                    key.to_string(),
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(CachedSeries { etag: format!("\"{key}\""), body: Arc::new(key.to_string()) })
                    },
                    None,
                )
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
            });
        }
        while let Some(res) = set.join_next().await {
            res.expect("join").expect("compute");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2, "distinct keys must each compute");
    }
}

/// Integration tests: run every `/series` query strategy against a real ClickHouse
/// (via testcontainers). Gracefully skip when Docker is unavailable so a plain
/// `cargo test` still passes locally; CI (with Docker) exercises them.
#[cfg(all(test, not(embedded_db)))]
mod ch_it {
    use super::*;
    use analytics_schema::ddl::ensure_telemetry_tables;
    use analytics_schema::telemetry::MetricRow;
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use testcontainers_modules::clickhouse::ClickHouse;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    /// ONE ClickHouse container shared by the whole module (per-container churn was
    /// flaky + slow). We share only the URL — a hyper-based `clickhouse::Client`
    /// is bound to the runtime it was built on, and `#[tokio::test]` gives each
    /// test its own runtime, so every test builds a fresh client from the URL.
    /// Started once on a dedicated thread (can't nest runtimes inside a test).
    /// `None` ⇒ Docker unavailable (skip). Tests scope data by unique org/metric.
    fn shared_url() -> Option<String> {
        static URL: OnceLock<Option<String>> = OnceLock::new();
        URL.get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::sync_channel::<Option<String>>(0);
            // A dedicated thread owns the container for the whole process and parks,
            // keeping the testcontainers Ryuk keep-alive connection open so the
            // container IS reaped on process exit (no `forget`, no leaked containers).
            let spawned = std::thread::Builder::new().name("ch-it-container".into()).spawn(move || {
                let rt = match tokio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(_) => {
                        let _ = tx.send(None);
                        return;
                    }
                };
                rt.block_on(async move {
                    let container = match ClickHouse::default().start().await {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("skip telemetry_series integration tests (no ClickHouse container): {e}");
                            let _ = tx.send(None);
                            return;
                        }
                    };
                    let url = match container.get_host_port_ipv4(8123).await {
                        Ok(port) => format!("http://127.0.0.1:{port}"),
                        Err(_) => {
                            let _ = tx.send(None);
                            return;
                        }
                    };
                    let client = clickhouse::Client::default().with_url(&url).with_database("default");
                    // Generic metric tables (no LLM/poll DDL) + a minimal endpoint_metrics.
                    if ensure_telemetry_tables(&client).await.is_err() {
                        let _ = tx.send(None);
                        return;
                    }
                    let _ = client
                        .query(
                            "CREATE TABLE IF NOT EXISTS analytics.endpoint_metrics \
                             (snapshot_time DateTime64(3), organization_uuid String, endpoint_uuid String, ops_per_sec Nullable(Float64), connected_clients Nullable(Float64), latency_p99_us Nullable(UInt64)) \
                             ENGINE = MergeTree ORDER BY (organization_uuid, endpoint_uuid, snapshot_time)",
                        )
                        .execute()
                        .await;
                    let _ = tx.send(Some(url));
                    // Hold the container (and its Ryuk keep-alive) until the process exits.
                    std::future::pending::<()>().await;
                });
            });
            if spawned.is_err() {
                return None;
            }
            rx.recv().ok().flatten()
        })
        .clone()
    }

    async fn client() -> Option<clickhouse::Client> {
        Some(clickhouse::Client::default().with_url(shared_url()?).with_database("default"))
    }

    fn metric_row(org: &str, node: &str, name: &str, kind: &str, ts: i64, value: Option<f64>, labels: Vec<(String, String)>) -> MetricRow {
        MetricRow {
            timestamp: DateTime::from_timestamp(ts, 0).unwrap(),
            organization_uuid: org.to_string(),
            service_name: "test".to_string(),
            node_uuid: node.to_string(),
            metric_name: name.to_string(),
            metric_kind: kind.to_string(),
            value,
            count: None,
            sum: None,
            bucket_bounds: vec![],
            bucket_counts: vec![],
            labels,
            scope: "test".to_string(),
        }
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct CountR {
        c: u64,
    }

    async fn seed(client: &clickhouse::Client, table: &str, rows: &[MetricRow]) {
        let mut ins = client.insert(table).expect("insert builder");
        for r in rows {
            ins.write(r).await.expect("write row");
        }
        ins.end().await.expect("flush insert");
        // Block until the freshly-inserted part is visible (insert→read can lag a
        // touch under load). The whole insert is one part, so any visible row means
        // all are. Cache disabled so the poll re-reads.
        if let Some(first) = rows.first() {
            let q = format!(
                "SELECT count() AS c FROM {table} WHERE organization_uuid = '{}' AND metric_name = '{}' SETTINGS use_query_cache = 0",
                escape_clickhouse_string(&first.organization_uuid),
                escape_clickhouse_string(&first.metric_name),
            );
            for _ in 0..30 {
                let r: Vec<CountR> = client.query(&q).fetch_all().await.unwrap_or_default();
                if r.first().map(|x| x.c).unwrap_or(0) > 0 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    fn make_request(metrics: Vec<Requested>, from: i64, step: i64, buckets: usize, scope: Option<(&'static str, String)>) -> SeriesRequest {
        SeriesRequest {
            metrics,
            from: DateTime::from_timestamp(from, 0).unwrap(),
            to: DateTime::from_timestamp(from + step * buckets as i64, 0).unwrap(),
            buckets,
            step_secs: step,
            grid_start_secs: (from / step) * step,
            scope_label: scope,
            scope_endpoint: None,
            traffic_class: TrafficClassFilter::All,
            endpoint_kind: None,
        }
    }

    /// Disable the ClickHouse query cache for test reads — otherwise the first
    /// (pre-visibility) empty result gets cached and every retry returns it.
    fn uncached(sql: &str) -> String {
        sql.replace("use_query_cache = 1", "use_query_cache = 0")
    }

    /// Run a scalar query, retrying briefly while the result is empty to absorb
    /// MergeTree insert→visibility lag (the shared container is fast but eventual).
    async fn run_scalar(client: &clickhouse::Client, sql: &str) -> HashMap<u32, f64> {
        let sql = uncached(sql);
        for attempt in 0..8 {
            let rows: Vec<ScalarRow> = client.query(&sql).fetch_all().await.expect("scalar query");
            if !rows.is_empty() || attempt == 7 {
                return rows.into_iter().filter_map(|r| r.value.map(|value| (r.bucket_ts, value))).collect();
            }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
        HashMap::new()
    }

    const FROM: i64 = 1_700_000_000;
    const STEP: i64 = 60;

    fn buckets() -> (u32, u32, u32) {
        let grid = (FROM / STEP) * STEP;
        (grid as u32, (grid + STEP) as u32, (grid + 2 * STEP) as u32)
    }

    #[tokio::test]
    async fn counter_is_per_node_window_diff_summed() {
        let Some(client) = client().await else { return };
        let org = "org-counter";
        // Two nodes, cumulative totals across three buckets.
        let rows = vec![
            metric_row(org, "n1", "t.counter", "sum", FROM + 1, Some(100.0), vec![]),
            metric_row(org, "n1", "t.counter", "sum", FROM + STEP + 1, Some(130.0), vec![]),
            metric_row(org, "n1", "t.counter", "sum", FROM + 2 * STEP + 1, Some(150.0), vec![]),
            metric_row(org, "n2", "t.counter", "sum", FROM + 1, Some(50.0), vec![]),
            metric_row(org, "n2", "t.counter", "sum", FROM + STEP + 1, Some(70.0), vec![]),
            metric_row(org, "n2", "t.counter", "sum", FROM + 2 * STEP + 1, Some(110.0), vec![]),
        ];
        seed(&client, "analytics.eden", &rows).await;
        let req = make_request(vec![], FROM, STEP, 3, None);
        let map = run_scalar(&client, &counter_sql(org, "t.counter", &req)).await;
        let (b0, b1, b2) = buckets();
        // First bucket has no prior cumulative → 0; then summed per-node deltas.
        assert!((map.get(&b0).copied().unwrap_or(0.0)).abs() < 0.01, "b0 {:?}", map.get(&b0));
        assert!((map.get(&b1).copied().unwrap_or(0.0) - 50.0).abs() < 0.01, "b1 {:?}", map.get(&b1)); // 30 + 20
        assert!((map.get(&b2).copied().unwrap_or(0.0) - 60.0).abs() < 0.01, "b2 {:?}", map.get(&b2)); // 20 + 40
    }

    #[tokio::test]
    async fn counter_reset_guard_treats_drop_as_own_value() {
        let Some(client) = client().await else { return };
        let org = "org-reset";
        // Node restarts between bucket 1 and 2 (cumulative drops 200 -> 5).
        let rows = vec![
            metric_row(org, "n1", "t.reset", "sum", FROM + 1, Some(120.0), vec![]),
            metric_row(org, "n1", "t.reset", "sum", FROM + STEP + 1, Some(200.0), vec![]),
            metric_row(org, "n1", "t.reset", "sum", FROM + 2 * STEP + 1, Some(5.0), vec![]),
        ];
        seed(&client, "analytics.eden", &rows).await;
        let req = make_request(vec![], FROM, STEP, 3, None);
        let map = run_scalar(&client, &counter_sql(org, "t.reset", &req)).await;
        let (_b0, b1, b2) = buckets();
        assert!((map.get(&b1).copied().unwrap_or(0.0) - 80.0).abs() < 0.01, "b1 {:?}", map.get(&b1)); // 200-120
        // Negative raw delta (5-200) → reset → counts the post-reset value (5).
        assert!((map.get(&b2).copied().unwrap_or(0.0) - 5.0).abs() < 0.01, "b2 {:?}", map.get(&b2));
    }

    #[tokio::test]
    async fn gauge_is_bucket_average() {
        let Some(client) = client().await else { return };
        let org = "org-gauge";
        let rows = vec![
            metric_row(org, "n1", "t.gauge", "gauge", FROM + 1, Some(10.0), vec![]),
            metric_row(org, "n1", "t.gauge", "gauge", FROM + 5, Some(20.0), vec![]),
            metric_row(org, "n1", "t.gauge", "gauge", FROM + STEP + 1, Some(30.0), vec![]),
        ];
        seed(&client, "analytics.eden", &rows).await;
        let req = make_request(vec![], FROM, STEP, 3, None);
        let map = run_scalar(&client, &gauge_sql(org, "t.gauge", &req)).await;
        let (b0, b1, _b2) = buckets();
        assert!((map.get(&b0).copied().unwrap_or(0.0) - 15.0).abs() < 0.01, "b0 {:?}", map.get(&b0)); // avg(10,20)
        assert!((map.get(&b1).copied().unwrap_or(0.0) - 30.0).abs() < 0.01, "b1 {:?}", map.get(&b1));
    }

    #[tokio::test]
    async fn histogram_merges_and_quantiles() {
        let Some(client) = client().await else { return };
        let org = "org-hist";
        let mut r1 = metric_row(org, "n1", "t.hist", "histogram", FROM + 1, None, vec![]);
        r1.count = Some(6);
        r1.bucket_bounds = vec![10.0, 20.0, 30.0];
        r1.bucket_counts = vec![1, 2, 2, 1];
        let mut r2 = metric_row(org, "n2", "t.hist", "histogram", FROM + 2, None, vec![]);
        r2.count = Some(4);
        r2.bucket_bounds = vec![10.0, 20.0, 30.0];
        r2.bucket_counts = vec![0, 2, 1, 1];
        seed(&client, "analytics.eden", &[r1, r2]).await;
        let req = make_request(vec![], FROM, STEP, 3, None);
        let sql = uncached(&histogram_sql(org, "t.hist", &req));
        let (b0, _b1, _b2) = buckets();
        let mut rows: Vec<HistRow> = Vec::new();
        for attempt in 0..8 {
            rows = client.query(&sql).fetch_all().await.expect("hist query");
            if !rows.is_empty() || attempt == 7 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
        let row = rows.iter().find(|r| r.bucket_ts == b0).expect("bucket0 hist");
        assert_eq!(row.counts, vec![1, 4, 3, 2]); // element-wise merge
        assert_eq!(row.total, 10);
        let p50 = hist_quantile(&row.counts, &row.bounds, row.total, 0.5).expect("p50");
        let p99 = hist_quantile(&row.counts, &row.bounds, row.total, 0.99).expect("p99");
        assert!(p50 > 10.0 && p50 < 30.0, "p50 {p50}");
        assert!(p99 >= p50, "p99 {p99} >= p50 {p50}");
    }

    #[tokio::test]
    async fn histogram_uses_window_delta_not_sum_of_cumulative_snapshots() {
        let Some(client) = client().await else { return };
        let org = "org-hist-delta";
        let bounds = vec![10.0, 20.0];

        let mut previous = metric_row(org, "n1", "t.hist.delta", "histogram", FROM + 1, None, vec![]);
        previous.count = Some(2);
        previous.bucket_bounds = bounds.clone();
        previous.bucket_counts = vec![0, 2, 0];

        let mut first_current = metric_row(org, "n1", "t.hist.delta", "histogram", FROM + STEP + 1, None, vec![]);
        first_current.count = Some(4);
        first_current.bucket_bounds = bounds.clone();
        first_current.bucket_counts = vec![0, 2, 2];

        let mut latest_current = metric_row(org, "n1", "t.hist.delta", "histogram", FROM + STEP + 2, None, vec![]);
        latest_current.count = Some(6);
        latest_current.bucket_bounds = bounds;
        latest_current.bucket_counts = vec![0, 2, 4];

        seed(&client, "analytics.eden", &[previous, first_current, latest_current]).await;
        let req = make_request(vec![], FROM, STEP, 3, None);
        let sql = uncached(&histogram_sql(org, "t.hist.delta", &req));
        let (_b0, b1, _b2) = buckets();
        let mut rows: Vec<HistRow> = Vec::new();
        for attempt in 0..8 {
            rows = client.query(&sql).fetch_all().await.expect("hist delta query");
            if !rows.is_empty() || attempt == 7 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
        let row = rows.iter().find(|r| r.bucket_ts == b1).expect("bucket1 hist");

        assert_eq!(row.counts, vec![0, 0, 4]);
        assert_eq!(row.total, 4);
    }

    #[tokio::test]
    async fn snapshot_reads_endpoint_metrics() {
        let Some(client) = client().await else { return };
        let org = "org-snap";
        let organization_uuid = org.to_string();
        let ms = (FROM + 1) * 1000;
        for ops in [1000.0, 2000.0] {
            let sql = format!(
                "INSERT INTO analytics.endpoint_metrics (snapshot_time, organization_uuid, endpoint_uuid, ops_per_sec) VALUES (fromUnixTimestamp64Milli({ms}), '{organization_uuid}', 'ep1', {ops})"
            );
            client.query(&sql).execute().await.expect("insert endpoint_metrics");
        }
        let _ = client.query("OPTIMIZE TABLE analytics.endpoint_metrics FINAL").execute().await;
        let req = make_request(vec![], FROM, STEP, 3, None);
        let value_expr = endpoint_metrics_expr("eden.analytics.endpoint.ops_per_sec").unwrap();
        let map = run_scalar(&client, &snapshot_sql(org, value_expr, &req)).await;
        let (b0, _b1, _b2) = buckets();
        assert!((map.get(&b0).copied().unwrap_or(0.0) - 1500.0).abs() < 0.5, "avg ops {:?}", map.get(&b0)); // avg(1000,2000)
    }

    #[tokio::test]
    async fn org_scope_isolates_tenants() {
        let Some(client) = client().await else { return };
        let rows = vec![
            metric_row("org-a", "n1", "t.iso", "sum", FROM + 1, Some(100.0), vec![]),
            metric_row("org-a", "n1", "t.iso", "sum", FROM + STEP + 1, Some(130.0), vec![]),
            metric_row("org-b", "n1", "t.iso", "sum", FROM + 1, Some(1000.0), vec![]),
            metric_row("org-b", "n1", "t.iso", "sum", FROM + STEP + 1, Some(9000.0), vec![]),
        ];
        seed(&client, "analytics.eden", &rows).await;
        let req = make_request(vec![], FROM, STEP, 3, None);
        let map = run_scalar(&client, &counter_sql("org-a", "t.iso", &req)).await;
        let (_b0, b1, _b2) = buckets();
        assert!((map.get(&b1).copied().unwrap_or(0.0) - 30.0).abs() < 0.01, "org-a only {:?}", map.get(&b1));
    }

    #[tokio::test]
    async fn scope_label_filters_to_object() {
        let Some(client) = client().await else { return };
        let org = "org-scope";
        let rows = vec![
            metric_row(org, "n1", "t.scoped", "gauge", FROM + 1, Some(10.0), vec![("endpoint_uuid".into(), "ep1".into())]),
            metric_row(org, "n1", "t.scoped", "gauge", FROM + 1, Some(99.0), vec![("endpoint_uuid".into(), "ep2".into())]),
        ];
        seed(&client, "analytics.eden", &rows).await;
        let req = make_request(vec![], FROM, STEP, 3, Some(("endpoint_uuid", "ep1".to_string())));
        let map = run_scalar(&client, &gauge_sql(org, "t.scoped", &req)).await;
        let (b0, _b1, _b2) = buckets();
        assert!((map.get(&b0).copied().unwrap_or(0.0) - 10.0).abs() < 0.01, "only ep1 {:?}", map.get(&b0));
    }

    #[tokio::test]
    async fn run_queries_assembles_columnar_batch() {
        let Some(client) = client().await else { return };
        let org = "org-batch";
        let mut h = metric_row(org, "n1", "t.lat", "histogram", FROM + 1, None, vec![]);
        h.count = Some(3);
        h.bucket_bounds = vec![10.0, 20.0];
        h.bucket_counts = vec![1, 1, 1];
        seed(
            &client,
            "analytics.eden",
            &[
                metric_row(org, "n1", "t.req", "sum", FROM + 1, Some(10.0), vec![]),
                metric_row(org, "n1", "t.req", "sum", FROM + STEP + 1, Some(40.0), vec![]),
                metric_row(org, "n1", "t.g", "gauge", FROM + 1, Some(7.0), vec![]),
                h,
            ],
        )
        .await;
        let metrics = vec![resolve("t.req", "Counter"), resolve("t.g", "Gauge"), resolve("t.lat", "Histogram")];
        let req = make_request(metrics, FROM, STEP, 3, None);
        let resp = run_queries(&client, org, &req, None).await.expect("run_queries");
        assert_eq!(resp.n, 3);
        assert_eq!(resp.step, STEP);
        let names: Vec<&str> = resp.series.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"t.req"));
        assert!(names.contains(&"t.g"));
        // histogram fans out to three percentile series.
        assert!(names.contains(&"t.lat:p50") && names.contains(&"t.lat:p95") && names.contains(&"t.lat:p99"));
        for s in &resp.series {
            assert_eq!(s.values.len(), 3, "series {} grid length", s.name);
        }
        let req_series = resp.series.iter().find(|s| s.name == "t.req").unwrap();
        assert_eq!(req_series.values[1], Some(30.0)); // counter delta in bucket 1
    }

    // ── Group B: additional integration coverage ──

    #[tokio::test]
    async fn empty_result_yields_full_grid() {
        let Some(client) = client().await else { return };
        // A metric with NO rows still produces a full, well-formed grid.
        let org = "org-empty";
        let metrics = vec![resolve("t.none.counter", "Counter"), resolve("t.none.gauge", "Gauge")];
        let req = make_request(metrics, FROM, STEP, 4, None);
        let resp = run_queries(&client, org, &req, None).await.expect("run_queries");
        assert_eq!(resp.n, 4);
        let counter = resp.series.iter().find(|s| s.name == "t.none.counter").expect("counter series");
        assert_eq!(counter.values.len(), 4);
        assert!(counter.values.iter().all(|v| *v == Some(0.0)), "counter gaps = 0: {:?}", counter.values);
        let gauge = resp.series.iter().find(|s| s.name == "t.none.gauge").expect("gauge series");
        assert_eq!(gauge.values.len(), 4);
        assert!(gauge.values.iter().all(|v| v.is_none()), "gauge gaps = null: {:?}", gauge.values);
    }

    #[tokio::test]
    async fn since_incremental_returns_aligned_tail() {
        let Some(client) = client().await else { return };
        let org = "org-since";
        // Cumulative counter over 6 buckets (+10 each).
        let rows: Vec<MetricRow> = (0..6)
            .map(|i| metric_row(org, "n1", "t.since", "sum", FROM + i * STEP + 1, Some(100.0 + (i as f64) * 10.0), vec![]))
            .collect();
        seed(&client, "analytics.eden", &rows).await;
        // An incremental fetch starts the window at bucket 3 (what `since` produces).
        let tail_from = FROM + 3 * STEP;
        let req = make_request(vec![], tail_from, STEP, 3, None);
        let grid = (tail_from / STEP) * STEP;
        assert_eq!(grid, tail_from, "tail window is step-aligned");
        let map = run_scalar(&client, &counter_sql(org, "t.since", &req)).await;
        let b_first = grid as u32;
        let b_second = (grid + STEP) as u32;
        // First bucket in the incremental window has no in-window predecessor → 0;
        // subsequent buckets recover the +10 per-bucket delta.
        assert!(
            (map.get(&b_first).copied().unwrap_or(-1.0)).abs() < 0.01,
            "first tail bucket {:?}",
            map.get(&b_first)
        );
        assert!(
            (map.get(&b_second).copied().unwrap_or(0.0) - 10.0).abs() < 0.01,
            "second tail bucket {:?}",
            map.get(&b_second)
        );
        // Only the tail (≤3 buckets) is present — none from before the window.
        assert!(
            map.keys().all(|&k| k >= b_first),
            "no buckets before the tail window: {:?}",
            map.keys().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn counter_multi_node_across_four_buckets() {
        let Some(client) = client().await else { return };
        let org = "org-mb";
        // Two nodes, uneven cumulative growth, four buckets.
        let n1 = [100.0, 130.0, 150.0, 160.0]; // deltas 30, 20, 10
        let n2 = [50.0, 70.0, 110.0, 200.0]; // deltas 20, 40, 90
        let mut rows = Vec::new();
        for (i, v) in n1.iter().enumerate() {
            rows.push(metric_row(org, "n1", "t.mb", "sum", FROM + (i as i64) * STEP + 1, Some(*v), vec![]));
        }
        for (i, v) in n2.iter().enumerate() {
            rows.push(metric_row(org, "n2", "t.mb", "sum", FROM + (i as i64) * STEP + 1, Some(*v), vec![]));
        }
        seed(&client, "analytics.eden", &rows).await;
        let req = make_request(vec![], FROM, STEP, 4, None);
        let map = run_scalar(&client, &counter_sql(org, "t.mb", &req)).await;
        let grid = (FROM / STEP) * STEP;
        let b = |i: i64| (grid + i * STEP) as u32;
        assert!((map.get(&b(0)).copied().unwrap_or(0.0)).abs() < 0.01, "b0 {:?}", map.get(&b(0)));
        assert!((map.get(&b(1)).copied().unwrap_or(0.0) - 50.0).abs() < 0.01, "b1 {:?}", map.get(&b(1))); // 30+20
        assert!((map.get(&b(2)).copied().unwrap_or(0.0) - 60.0).abs() < 0.01, "b2 {:?}", map.get(&b(2))); // 20+40
        assert!((map.get(&b(3)).copied().unwrap_or(0.0) - 100.0).abs() < 0.01, "b3 {:?}", map.get(&b(3))); // 10+90
    }

    #[tokio::test]
    async fn snapshot_scope_filters_to_endpoint() {
        let Some(client) = client().await else { return };
        let org = "org-snap-scope";
        let organization_uuid = org.to_string();
        let ms = (FROM + 1) * 1000;
        for (ep, ops) in [("epA", 1000.0), ("epB", 5000.0)] {
            let sql = format!(
                "INSERT INTO analytics.endpoint_metrics (snapshot_time, organization_uuid, endpoint_uuid, ops_per_sec) VALUES (fromUnixTimestamp64Milli({ms}), '{organization_uuid}', '{ep}', {ops})"
            );
            client.query(&sql).execute().await.expect("insert endpoint_metrics");
        }
        let _ = client.query("OPTIMIZE TABLE analytics.endpoint_metrics FINAL").execute().await;
        let req = make_request(vec![], FROM, STEP, 3, Some(("endpoint_uuid", "epA".to_string())));
        let value_expr = endpoint_metrics_expr("eden.analytics.endpoint.ops_per_sec").unwrap();
        let map = run_scalar(&client, &snapshot_sql(org, value_expr, &req)).await;
        let (b0, _b1, _b2) = buckets();
        // Only epA's snapshot (1000) is in scope — epB (5000) is excluded.
        assert!((map.get(&b0).copied().unwrap_or(0.0) - 1000.0).abs() < 0.5, "only epA {:?}", map.get(&b0));
    }
}
