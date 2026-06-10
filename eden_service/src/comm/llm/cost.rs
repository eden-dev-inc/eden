//! Cost observability endpoints.
//!
//! Two read endpoints, both dashboard-JWT authed and organization-scoped to the
//! caller's org (mirroring [`super::gateway_dashboard::get_dashboard`]):
//!
//! - `GET /api/v1/llm/cost/timeseries` — as-spent LLM cost over a configurable
//!   time range + bucket (a generalization of the gateway dashboard's hardcoded
//!   current-month daily query), optionally grouped by provider or model.
//! - `GET /api/v1/llm/pricing` — current per-model provider prices (live
//!   OpenRouter cache merged over the static fallback table).

use actix_web::{HttpResponse, Responder, web};
use analytics_schema::llm::tables as llm_tables;
use chrono::{DateTime, Datelike, Duration, Utc};
use clickhouse::Row;
use database::db::methods::llm::{StoredLlmGatewayRouteRollup, StoredLlmGatewayUsageRollup};
use eden_core::auth::ParsedJwt;
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{EdenUuid, OrganizationUuid};
use eden_core::response::EdenResponse;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

use crate::comm::rbac::verify_control_perms;
use crate::comm::telemetry_analytics::{optional_param, parse_optional_time, parse_range_secs};
use crate::{EdenDb, error_handling};

const DEFAULT_RANGE_SECS: i64 = 30 * 24 * 60 * 60; // 30 days
/// Cap on the number of buckets a single query may produce (guards the scan /
/// response size for silly range÷bucket combinations).
const MAX_BUCKETS: i64 = 1500;
/// Cap on grouped series returned (top-N by cost).
const MAX_GROUPS: usize = 25;

// ── response types ───────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct CostPoint {
    /// Bucket start (RFC3339 via chrono Serialize).
    pub bucket: DateTime<Utc>,
    pub cost_micros: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub request_count: u64,
}

#[derive(Debug, Serialize, PartialEq, ToSchema)]
pub struct CostGroupSeries {
    /// `provider` or `provider/model`, depending on `group_by`.
    pub key: String,
    pub cost_micros: u64,
    pub points: Vec<CostPoint>,
}

#[derive(Debug, Serialize, PartialEq, ToSchema)]
pub struct CostTimeseriesResponse {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub bucket_seconds: i64,
    pub total_cost_micros: u64,
    pub points: Vec<CostPoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_group: Option<Vec<CostGroupSeries>>,
}

// ── ClickHouse row shapes ────────────────────────────────────────

#[derive(Debug, Row, serde::Deserialize)]
struct CostBucketRow {
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    bucket: DateTime<Utc>,
    cost_micros: u64,
    prompt_tokens: u64,
    completion_tokens: u64,
    request_count: u64,
}

#[derive(Debug, Row, serde::Deserialize)]
struct CostGroupBucketRow {
    group_key: String,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    bucket: DateTime<Utc>,
    cost_micros: u64,
    prompt_tokens: u64,
    completion_tokens: u64,
    request_count: u64,
}

// ── helpers ──────────────────────────────────────────────────────

/// Translate a bucket label to seconds. Restricted to a fixed set so the value
/// can be safely embedded as a SQL literal.
fn bucket_seconds(raw: Option<&str>) -> i64 {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("1h" | "hour" | "hourly") => 3_600,
        Some("6h") => 21_600,
        Some("12h") => 43_200,
        Some("1d" | "day" | "daily") | None => 86_400,
        Some("1w" | "week" | "weekly") => 604_800,
        _ => 86_400,
    }
}

/// Map a `group_by` param to the ClickHouse column expression, or `None` for an
/// ungrouped query. Restricted to a fixed set (no injection).
fn group_column(raw: Option<&str>) -> Option<&'static str> {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("provider") => Some("provider"),
        Some("model") => Some("concat(provider, '/', model)"),
        _ => None,
    }
}

/// Floor a timestamp to the nearest bucket boundary (aligned to the Unix epoch,
/// matching ClickHouse `toStartOfInterval`).
fn floor_to_bucket(ts: DateTime<Utc>, bucket_secs: i64) -> DateTime<Utc> {
    let epoch = ts.timestamp();
    let floored = epoch - epoch.rem_euclid(bucket_secs);
    DateTime::from_timestamp(floored, 0).unwrap_or(ts)
}

fn month_bucket(ts: DateTime<Utc>) -> i32 {
    ts.year().saturating_mul(100).saturating_add(ts.month() as i32)
}

const OPERATION_FILTER: &str = "operation IN ('chat.completions', 'chat.completions.stream', 'responses')";

pub(crate) fn llm_analytics_organization_uuid(org_uuid: &OrganizationUuid) -> String {
    org_uuid.uuid().to_string()
}

/// SQL predicate for the requested traffic source/class.
///
/// `traffic_source` remains the exact backend wire value filter. `traffic_class`
/// is the shared observability filter used by metrics/logs/traces; it maps onto
/// the known LLM traffic-source taxonomy. Historical callers that omit both
/// filters still default to `proxy_app`; callers that send `traffic_class=all`
/// explicitly span every source.
pub(crate) fn traffic_source_filter(raw_source: Option<&str>, raw_class: Option<&str>) -> Result<String, actix_web::Error> {
    let class_clause = traffic_class_clause(raw_class)?;
    let source_clause = traffic_source_clause(raw_source, raw_class.is_some());
    let mut clauses = Vec::new();
    if let Some(source_clause) = source_clause {
        clauses.push(source_clause.to_string());
    }
    if let Some(class_clause) = class_clause {
        clauses.push(class_clause.to_string());
    }
    clauses.push(OPERATION_FILTER.to_string());
    Ok(clauses.join(" AND "))
}

fn traffic_source_clause(raw: Option<&str>, class_was_sent: bool) -> Option<&'static str> {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("all") => None,
        Some("llm_gateway") => Some("traffic_source = 'llm_gateway'"),
        Some("agent_gateway") => Some("traffic_source = 'agent_gateway'"),
        Some("proxy_app") => Some("traffic_source = 'proxy_app'"),
        Some("internal_job") => Some("traffic_source = 'internal_job'"),
        None | Some("") if class_was_sent => None,
        None | Some("") | Some(_) => Some("traffic_source = 'proxy_app'"),
    }
}

fn traffic_class_clause(raw: Option<&str>) -> Result<Option<&'static str>, actix_web::Error> {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("all") => Ok(None),
        Some("external") => Ok(Some("traffic_source IN ('llm_gateway', 'agent_gateway', 'proxy_app')")),
        Some("internal") => Ok(Some("traffic_source = 'internal_job'")),
        Some(other) => Err(actix_web::error::ErrorBadRequest(format!(
            "invalid traffic_class: {other}; expected external, internal, or all"
        ))),
    }
}

fn rollup_fallback_enabled(raw_source: Option<&str>, raw_class: Option<&str>) -> bool {
    if matches!(raw_class.map(|s| s.trim().to_ascii_lowercase()).as_deref(), Some("internal")) {
        return false;
    }
    match raw_source.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("llm_gateway" | "agent_gateway" | "internal_job") => false,
        None | Some("") | Some("all" | "proxy_app") | Some(_) => true,
    }
}

fn has_activity(points: &[CostPoint]) -> bool {
    points
        .iter()
        .any(|point| point.cost_micros > 0 || point.prompt_tokens > 0 || point.completion_tokens > 0 || point.request_count > 0)
}

// ── Endpoint A: cost time-series ─────────────────────────────────

/// LLM cost time-series (as-spent) over a configurable range + bucket.
///
/// Query params: `from`/`to` (RFC3339) or `range` (e.g. `7d`, default 30d);
/// `bucket` (`1h`/`6h`/`1d`/`1w`, default `1d`); optional `traffic_source`,
/// `traffic_class` (`external` | `internal` | `all`), and `group_by`
/// (`provider` | `model`).
///
/// **Permissions**: `ControlPerms::READ`.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["LLM"],
    path = "/llm/cost/timeseries",
    operation_id = "llm_cost_timeseries",
    responses((status = OK, body = CostTimeseriesResponse))
)]
pub async fn cost_timeseries(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    query: web::Query<HashMap<String, String>>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let organization_uuid = llm_analytics_organization_uuid(auth.org_uuid());

    let params = query.into_inner();

    let bucket_secs = bucket_seconds(optional_param(&params, "bucket"));
    let to = parse_optional_time(&params, "to")?.unwrap_or_else(Utc::now);
    let from = match parse_optional_time(&params, "from")? {
        Some(from) => from,
        None => {
            let range = parse_range_secs(optional_param(&params, "range"))?.unwrap_or(DEFAULT_RANGE_SECS);
            to - Duration::seconds(range)
        }
    };
    if from > to {
        return Err(actix_web::error::ErrorBadRequest("from must be before or equal to to"));
    }

    let aligned_from = floor_to_bucket(from, bucket_secs);
    let span_secs = (to - aligned_from).num_seconds().max(0);
    let n_buckets = span_secs / bucket_secs + 1;
    if n_buckets > MAX_BUCKETS {
        return Err(actix_web::error::ErrorBadRequest(format!(
            "range/bucket would produce {n_buckets} buckets (max {MAX_BUCKETS}); widen the bucket or shorten the range"
        )));
    }

    let group_col = group_column(optional_param(&params, "group_by"));
    let event_filter = traffic_source_filter(optional_param(&params, "traffic_source"), optional_param(&params, "traffic_class"))?;

    let client = database
        .clickhouse_pool()
        .get()
        .await
        .map_err(|e| actix_web::error::ErrorServiceUnavailable(format!("analytics backend unavailable: {e}")))?;

    let from_str = from.to_rfc3339();
    let to_str = to.to_rfc3339();

    // ── total series (always computed) ──
    let total_sql = format!(
        r#"
        SELECT
            toStartOfInterval(timestamp, INTERVAL {bucket} SECOND) AS bucket,
            sum(estimated_cost_micros) AS cost_micros,
            sum(prompt_tokens) AS prompt_tokens,
            sum(completion_tokens) AS completion_tokens,
            count() AS request_count
        FROM {table}
        WHERE organization_uuid = ?
          AND timestamp BETWEEN parseDateTime64BestEffort(?) AND parseDateTime64BestEffort(?)
          AND {filter}
        GROUP BY bucket
        ORDER BY bucket
        "#,
        bucket = bucket_secs,
        table = llm_tables::LLM_OPERATION_EVENTS,
        filter = event_filter,
    );
    let total_rows = client
        .query(&total_sql)
        .bind(&organization_uuid)
        .bind(&from_str)
        .bind(&to_str)
        .fetch_all::<CostBucketRow>()
        .await
        .map_err(|e| {
            log::error!("cost timeseries query failed: {e}");
            actix_web::error::ErrorInternalServerError("failed to query cost timeseries")
        })?;

    let mut total_by_bucket: HashMap<i64, &CostBucketRow> = HashMap::with_capacity(total_rows.len());
    for row in &total_rows {
        total_by_bucket.insert(row.bucket.timestamp(), row);
    }
    let mut points = gap_filled(aligned_from, to, bucket_secs, |ts| {
        total_by_bucket.get(&ts).map(|r| CostPoint {
            bucket: DateTime::from_timestamp(ts, 0).unwrap_or(aligned_from),
            cost_micros: r.cost_micros,
            prompt_tokens: r.prompt_tokens,
            completion_tokens: r.completion_tokens,
            request_count: r.request_count,
        })
    });
    let mut total_cost_micros: u64 = points.iter().map(|p| p.cost_micros).sum();

    // ── grouped series (optional) ──
    let mut by_group = if let Some(col) = group_col {
        let grouped_sql = format!(
            r#"
            SELECT
                {col} AS group_key,
                toStartOfInterval(timestamp, INTERVAL {bucket} SECOND) AS bucket,
                sum(estimated_cost_micros) AS cost_micros,
                sum(prompt_tokens) AS prompt_tokens,
                sum(completion_tokens) AS completion_tokens,
                count() AS request_count
            FROM {table}
            WHERE organization_uuid = ?
              AND timestamp BETWEEN parseDateTime64BestEffort(?) AND parseDateTime64BestEffort(?)
              AND {filter}
            GROUP BY group_key, bucket
            ORDER BY bucket
            "#,
            col = col,
            bucket = bucket_secs,
            table = llm_tables::LLM_OPERATION_EVENTS,
            filter = event_filter,
        );
        let grouped_rows = client
            .query(&grouped_sql)
            .bind(&organization_uuid)
            .bind(&from_str)
            .bind(&to_str)
            .fetch_all::<CostGroupBucketRow>()
            .await
            .map_err(|e| {
                log::error!("grouped cost timeseries query failed: {e}");
                actix_web::error::ErrorInternalServerError("failed to query grouped cost timeseries")
            })?;
        Some(assemble_groups(grouped_rows, aligned_from, to, bucket_secs))
    } else {
        None
    };

    if !has_activity(&points)
        && rollup_fallback_enabled(optional_param(&params, "traffic_source"), optional_param(&params, "traffic_class"))
    {
        let rollups = database
            .list_llm_gateway_usage_rollups(auth.org_uuid(), month_bucket(to), 500, telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;
        let selected = preferred_rollups(&rollups);
        let routes = if group_col.is_some() && !selected.is_empty() {
            database
                .list_llm_gateway_route_rollups(auth.org_uuid(), 500, telemetry_wrapper)
                .await
                .map_err(|e| error_handling(e, &mut span))?
        } else {
            Vec::new()
        };
        if let Some(fallback) = rollup_fallback_from_rows(&selected, &routes, aligned_from, to, bucket_secs, group_col) {
            points = fallback.points;
            total_cost_micros = fallback.total_cost_micros;
            by_group = fallback.by_group;
        }
    }

    let response = CostTimeseriesResponse {
        from,
        to,
        bucket_seconds: bucket_secs,
        total_cost_micros,
        points,
        by_group,
    };
    Ok(HttpResponse::Ok().json(EdenResponse::response(response)))
}

struct RollupFallback {
    points: Vec<CostPoint>,
    total_cost_micros: u64,
    by_group: Option<Vec<CostGroupSeries>>,
}

fn preferred_rollups(rollups: &[StoredLlmGatewayUsageRollup]) -> Vec<&StoredLlmGatewayUsageRollup> {
    for kind in ["organization", "api_key", "consumer"] {
        let selected = rollups.iter().filter(|row| row.consumer_kind == kind && rollup_has_activity(row)).collect::<Vec<_>>();
        if !selected.is_empty() {
            return selected;
        }
    }
    Vec::new()
}

fn rollup_has_activity(row: &StoredLlmGatewayUsageRollup) -> bool {
    row.request_count > 0 || row.prompt_tokens > 0 || row.completion_tokens > 0 || row.estimated_cost_micros > 0
}

fn rollup_fallback_from_rows(
    rollups: &[&StoredLlmGatewayUsageRollup],
    routes: &[StoredLlmGatewayRouteRollup],
    aligned_from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket_secs: i64,
    group_col: Option<&str>,
) -> Option<RollupFallback> {
    let bucket = floor_to_bucket(to, bucket_secs);
    let mut total = CostPoint {
        bucket,
        cost_micros: 0,
        prompt_tokens: 0,
        completion_tokens: 0,
        request_count: 0,
    };

    let mut grouped: BTreeMap<String, CostGroupBucketRow> = BTreeMap::new();
    for row in rollups {
        total.cost_micros = total.cost_micros.saturating_add(row.estimated_cost_micros);
        total.prompt_tokens = total.prompt_tokens.saturating_add(row.prompt_tokens);
        total.completion_tokens = total.completion_tokens.saturating_add(row.completion_tokens);
        total.request_count = total.request_count.saturating_add(row.request_count);

        if let Some(group_col) = group_col {
            let key = rollup_group_key(row, routes, group_col);
            let entry = grouped.entry(key.clone()).or_insert_with(|| CostGroupBucketRow {
                group_key: key,
                bucket,
                cost_micros: 0,
                prompt_tokens: 0,
                completion_tokens: 0,
                request_count: 0,
            });
            entry.cost_micros = entry.cost_micros.saturating_add(row.estimated_cost_micros);
            entry.prompt_tokens = entry.prompt_tokens.saturating_add(row.prompt_tokens);
            entry.completion_tokens = entry.completion_tokens.saturating_add(row.completion_tokens);
            entry.request_count = entry.request_count.saturating_add(row.request_count);
        }
    }

    if !has_activity(std::slice::from_ref(&total)) {
        return None;
    }

    let bucket_ts = bucket.timestamp();
    let points = gap_filled(aligned_from, to, bucket_secs, |ts| if ts == bucket_ts { Some(total.clone()) } else { None });
    let total_cost_micros = points.iter().map(|point| point.cost_micros).sum();
    let by_group = group_col.map(|_| assemble_groups(grouped.into_values().collect(), aligned_from, to, bucket_secs));

    Some(RollupFallback { points, total_cost_micros, by_group })
}

fn rollup_group_key(row: &StoredLlmGatewayUsageRollup, routes: &[StoredLlmGatewayRouteRollup], group_col: &str) -> String {
    let route = row.endpoint_uuid.and_then(|endpoint_uuid| routes.iter().find(|route| route.endpoint_uuid == endpoint_uuid));
    match group_col {
        "provider" => route.map(|route| route.provider.clone()).unwrap_or_else(|| "unknown".to_string()),
        "concat(provider, '/', model)" => route
            .map(|route| format!("{}/{}", route.provider, route.model))
            .or_else(|| row.endpoint_uuid.map(|endpoint_uuid| endpoint_uuid.to_string()))
            .unwrap_or_else(|| "unknown".to_string()),
        _ => "unknown".to_string(),
    }
}

/// Produce evenly-spaced buckets from `aligned_from` up to `to`, filling missing
/// buckets with a zero point.
fn gap_filled<F>(aligned_from: DateTime<Utc>, to: DateTime<Utc>, bucket_secs: i64, mut lookup: F) -> Vec<CostPoint>
where
    F: FnMut(i64) -> Option<CostPoint>,
{
    let mut out = Vec::new();
    let mut ts = aligned_from.timestamp();
    let end = to.timestamp();
    while ts <= end {
        let point = lookup(ts).unwrap_or_else(|| CostPoint {
            bucket: DateTime::from_timestamp(ts, 0).unwrap_or(aligned_from),
            cost_micros: 0,
            prompt_tokens: 0,
            completion_tokens: 0,
            request_count: 0,
        });
        out.push(point);
        ts += bucket_secs;
    }
    out
}

/// Group rows by `group_key`, gap-fill each series, keep the top-N by total cost.
fn assemble_groups(
    rows: Vec<CostGroupBucketRow>,
    aligned_from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket_secs: i64,
) -> Vec<CostGroupSeries> {
    // group_key -> (bucket_ts -> row)
    let mut by_key: BTreeMap<String, HashMap<i64, CostGroupBucketRow>> = BTreeMap::new();
    for row in rows {
        by_key.entry(row.group_key.clone()).or_default().insert(row.bucket.timestamp(), row);
    }

    let mut series: Vec<CostGroupSeries> = by_key
        .into_iter()
        .map(|(key, buckets)| {
            let points = gap_filled(aligned_from, to, bucket_secs, |ts| {
                buckets.get(&ts).map(|r| CostPoint {
                    bucket: DateTime::from_timestamp(ts, 0).unwrap_or(aligned_from),
                    cost_micros: r.cost_micros,
                    prompt_tokens: r.prompt_tokens,
                    completion_tokens: r.completion_tokens,
                    request_count: r.request_count,
                })
            });
            let cost_micros = points.iter().map(|p| p.cost_micros).sum();
            CostGroupSeries { key, cost_micros, points }
        })
        .collect();

    series.sort_by(|a, b| b.cost_micros.cmp(&a.cost_micros));
    series.truncate(MAX_GROUPS);
    series
}

// ── Endpoint B: current model prices ─────────────────────────────

#[derive(Debug, Serialize, PartialEq, ToSchema)]
pub struct PricingResponse {
    pub prices: Vec<PriceRow>,
}

#[derive(Debug, Serialize, PartialEq, ToSchema)]
pub struct PriceRow {
    pub provider: String,
    pub model: String,
    pub source: String,
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
}

/// Current per-model provider prices (live OpenRouter cache over static fallback).
///
/// **Permissions**: `ControlPerms::READ`.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["LLM"],
    path = "/llm/pricing",
    operation_id = "llm_pricing",
    responses((status = OK, body = PricingResponse))
)]
pub async fn pricing(auth: web::ReqData<ParsedJwt>, database: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let prices = endpoint_core::llm_core::pricing::current_pricings()
        .into_iter()
        .map(|p| PriceRow {
            provider: p.provider,
            model: p.model,
            source: p.source.as_str().to_string(),
            input_micros_per_million: p.input_micros_per_million,
            output_micros_per_million: p.output_micros_per_million,
        })
        .collect();

    Ok(HttpResponse::Ok().json(EdenResponse::response(PricingResponse { prices })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoint_core::llm_core::TrafficSource;

    #[test]
    fn bucket_seconds_known_and_default() {
        assert_eq!(bucket_seconds(Some("1h")), 3_600);
        assert_eq!(bucket_seconds(Some("6h")), 21_600);
        assert_eq!(bucket_seconds(Some("1d")), 86_400);
        assert_eq!(bucket_seconds(Some("1w")), 604_800);
        assert_eq!(bucket_seconds(None), 86_400);
        assert_eq!(bucket_seconds(Some("garbage")), 86_400);
    }

    #[test]
    fn traffic_source_filter_defaults_to_proxy_app() {
        // Absent / empty / explicit proxy_app all preserve historical behavior.
        assert!(traffic_source_filter(None, None).expect("valid filter").starts_with("traffic_source = 'proxy_app'"));
        assert!(traffic_source_filter(Some(""), None).expect("valid filter").starts_with("traffic_source = 'proxy_app'"));
        assert!(traffic_source_filter(Some("proxy_app"), None).expect("valid filter").starts_with("traffic_source = 'proxy_app'"));
        assert!(traffic_source_filter(Some("typo"), None).expect("valid filter").starts_with("traffic_source = 'proxy_app'"));
        // Known sources map to the authoritative endpoint-core wire values.
        for source in [
            TrafficSource::LlmGateway,
            TrafficSource::AgentGateway,
            TrafficSource::ProxyApp,
            TrafficSource::InternalJob,
        ] {
            let expected = format!("traffic_source = '{}'", source.as_str());
            assert!(traffic_source_filter(Some(source.as_str()), None).expect("valid filter").starts_with(&expected));
        }
        // `all` explicitly spans every source — no traffic_source clause.
        assert!(!traffic_source_filter(Some("all"), None).expect("valid filter").contains("traffic_source"));
        // Operation filter is always present.
        assert!(traffic_source_filter(None, None).expect("valid filter").contains("operation IN"));
        assert!(traffic_source_filter(Some("all"), None).expect("valid filter").contains("operation IN"));
    }

    #[test]
    fn traffic_class_filter_maps_shared_observability_origin() {
        let all = traffic_source_filter(None, Some("all")).expect("valid filter");
        assert_eq!(all, OPERATION_FILTER);

        let external = traffic_source_filter(None, Some("external")).expect("valid filter");
        assert!(external.contains("llm_gateway"));
        assert!(external.contains("proxy_app"));
        assert!(!external.contains("internal_job"));

        let internal = traffic_source_filter(None, Some("internal")).expect("valid filter");
        assert!(internal.contains("internal_job"));
        assert!(!internal.contains("proxy_app"));

        assert!(traffic_source_filter(None, Some("unknown")).is_err());
    }

    #[test]
    fn rollup_fallback_is_limited_to_proxy_app_cost_views() {
        assert!(rollup_fallback_enabled(None, None));
        assert!(rollup_fallback_enabled(Some(""), None));
        assert!(rollup_fallback_enabled(Some("proxy_app"), None));
        assert!(rollup_fallback_enabled(Some("all"), None));
        assert!(rollup_fallback_enabled(Some("typo"), None));
        assert!(rollup_fallback_enabled(None, Some("external")));
        assert!(!rollup_fallback_enabled(None, Some("internal")));
        assert!(!rollup_fallback_enabled(Some("agent_gateway"), None));
        assert!(!rollup_fallback_enabled(Some("llm_gateway"), None));
    }

    #[test]
    fn analytics_organization_uuid_uses_raw_uuid_storage_format() {
        let org = OrganizationUuid::from(uuid::Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("uuid"));
        assert_eq!(llm_analytics_organization_uuid(&org), "12345678-1234-5678-1234-567812345678");
    }

    #[test]
    fn group_column_fixed_set() {
        assert_eq!(group_column(Some("provider")), Some("provider"));
        assert_eq!(group_column(Some("model")), Some("concat(provider, '/', model)"));
        assert_eq!(group_column(Some("user")), None);
        assert_eq!(group_column(None), None);
    }

    #[test]
    fn floor_aligns_to_bucket() {
        // 2026-01-01T00:00:30Z floored to 60s -> 00:00:00
        let ts = DateTime::from_timestamp(1_767_225_630, 0).expect("ts");
        let floored = floor_to_bucket(ts, 60);
        assert_eq!(floored.timestamp() % 60, 0);
        assert!(floored <= ts);
    }

    #[test]
    fn gap_fill_produces_evenly_spaced_buckets() {
        let from = DateTime::from_timestamp(0, 0).expect("from");
        let to = DateTime::from_timestamp(3 * 86_400, 0).expect("to");
        let points = gap_filled(from, to, 86_400, |_| None);
        assert_eq!(points.len(), 4); // 0,1,2,3 days inclusive
        for (i, p) in points.iter().enumerate() {
            assert_eq!(p.bucket.timestamp(), (i as i64) * 86_400);
            assert_eq!(p.cost_micros, 0);
        }
    }

    #[test]
    fn assemble_groups_sorts_by_cost_desc() {
        let from = DateTime::from_timestamp(0, 0).expect("from");
        let to = DateTime::from_timestamp(86_400, 0).expect("to");
        let rows = vec![
            CostGroupBucketRow {
                group_key: "cheap".into(),
                bucket: from,
                cost_micros: 10,
                prompt_tokens: 1,
                completion_tokens: 1,
                request_count: 1,
            },
            CostGroupBucketRow {
                group_key: "pricey".into(),
                bucket: from,
                cost_micros: 1000,
                prompt_tokens: 1,
                completion_tokens: 1,
                request_count: 1,
            },
        ];
        let series = assemble_groups(rows, from, to, 86_400);
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].key, "pricey");
        assert_eq!(series[1].key, "cheap");
        // each gap-filled to 2 buckets (day 0 + day 1)
        assert_eq!(series[0].points.len(), 2);
    }

    #[test]
    fn rollup_fallback_prefers_organization_rows_and_groups_by_model() {
        let org = OrganizationUuid::from(uuid::Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("org"));
        let endpoint = uuid::Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("endpoint");
        let from = DateTime::from_timestamp(1_767_225_600, 0).expect("from");
        let to = from + Duration::hours(1);
        let updated_at = to;
        let rollups = vec![
            StoredLlmGatewayUsageRollup {
                organization_uuid: org.clone(),
                consumer_kind: "organization".to_string(),
                consumer_id: org.uuid().to_string(),
                month_bucket: 202601,
                endpoint_uuid: Some(endpoint),
                request_count: 5,
                prompt_tokens: 50,
                completion_tokens: 70,
                total_tokens: 120,
                estimated_cost_micros: 9,
                cache_hit_count: 0,
                kv_cache_hit_count: 0,
                rate_limited_count: 0,
                updated_at,
            },
            StoredLlmGatewayUsageRollup {
                organization_uuid: org.clone(),
                consumer_kind: "api_key".to_string(),
                consumer_id: "key".to_string(),
                month_bucket: 202601,
                endpoint_uuid: Some(endpoint),
                request_count: 5,
                prompt_tokens: 50,
                completion_tokens: 70,
                total_tokens: 120,
                estimated_cost_micros: 9,
                cache_hit_count: 0,
                kv_cache_hit_count: 0,
                rate_limited_count: 0,
                updated_at,
            },
        ];
        let routes = vec![StoredLlmGatewayRouteRollup {
            organization_uuid: org,
            endpoint_uuid: endpoint,
            provider: "openai".to_string(),
            model: "showcase-chat-fast".to_string(),
            route_class: "default".to_string(),
            success_count: 5,
            error_count: 0,
            total_latency_ms: 10,
            min_latency_ms: 1,
            max_latency_ms: 3,
            total_output_tokens: 70,
            total_duration_ms: 10,
            first_observed_at: from,
            last_observed_at: to,
            updated_at,
        }];

        let selected = preferred_rollups(&rollups);
        let fallback = rollup_fallback_from_rows(&selected, &routes, from, to, 3_600, group_column(Some("model"))).expect("fallback");

        assert_eq!(fallback.total_cost_micros, 9);
        assert_eq!(fallback.points.iter().map(|point| point.request_count).sum::<u64>(), 5);
        let groups = fallback.by_group.expect("groups");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].key, "openai/showcase-chat-fast");
        assert_eq!(groups[0].points.iter().map(|point| point.request_count).sum::<u64>(), 5);
    }
}
