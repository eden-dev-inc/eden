//! Gateway request-history endpoint.
//!
//! `GET /api/v1/llm/gateway/requests` returns a paginated, filterable log of
//! individual LLM gateway requests straight from `analytics.llm_operation_events`.
//!
//! Query params:
//! - `from`/`to` (RFC3339) or `range` (e.g. `24h`, `7d`; default 7d)
//! - `traffic_source` (`proxy_app` default, or a `TrafficSource` wire value)
//! - `traffic_class` (`external` | `internal` | `all`)
//! - `provider`, `model`, `consumer_id` (exact match)
//! - `status` (`ok` | `error`)
//! - `search` (case-insensitive substring over provider/model)
//! - `limit` (default 50, cap 200), `offset` (default 0)
//!
//! **Permissions**: `ControlPerms::READ`.

use actix_web::{HttpResponse, Responder, web};
use analytics_schema::llm::{LlmOperationEventRow, tables as llm_tables};
use chrono::{DateTime, Duration, Utc};
use clickhouse::Row;
use eden_core::auth::ParsedJwt;
use eden_core::format::{EdenUuid, rbac::ControlPerms};
use eden_core::response::EdenResponse;
use endpoint_core::llm_core::LlmOperationEvent;
use endpoint_core::llm_core::analytics::recent_llm_operations;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

use crate::comm::rbac::verify_control_perms;
use crate::comm::telemetry_analytics::{optional_param, parse_optional_time, parse_range_secs};
use crate::{EdenDb, error_handling};

/// Default lookback when no `from`/`range` is supplied (7 days).
const DEFAULT_RANGE_SECS: i64 = 7 * 24 * 60 * 60;
/// Hard cap on rows returned in a single page.
const MAX_LIMIT: u32 = 200;
const DEFAULT_LIMIT: u32 = 50;

// ── response types ───────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, PartialEq, ToSchema)]
pub struct GatewayRequestRow {
    /// Event timestamp (RFC3339).
    pub timestamp: String,
    pub endpoint_uuid: String,
    pub provider: String,
    pub model: String,
    pub operation: String,
    pub traffic_source: String,
    pub consumer_id: String,
    pub agent_uuid: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub estimated_cost_micros: u64,
    pub estimated_arbitrage_savings_micros: u64,
    pub arbitrage_reason: String,
    pub cache_status: String,
    pub route_optimization_mode: String,
    pub latency_ms: u64,
    pub success: bool,
    pub error_message: String,
    pub streaming: bool,
    pub tool_used: bool,
    pub pii_detected: bool,
    pub pii_types: String,
    /// Governance action taken (allow/audit_only/redact/block).
    pub policy_action: String,
}

impl From<LlmGatewayRequestEventResponse> for GatewayRequestRow {
    fn from(e: LlmGatewayRequestEventResponse) -> Self {
        Self {
            timestamp: e.timestamp.to_rfc3339(),
            endpoint_uuid: e.endpoint_uuid,
            provider: e.provider,
            model: e.model,
            operation: e.operation,
            traffic_source: e.traffic_source,
            consumer_id: e.consumer_id.unwrap_or_default(),
            agent_uuid: e.agent_uuid.unwrap_or_default(),
            prompt_tokens: u64::from(e.prompt_tokens),
            completion_tokens: u64::from(e.completion_tokens),
            total_tokens: u64::from(e.total_tokens),
            request_bytes: u64::from(e.request_bytes),
            response_bytes: u64::from(e.response_bytes),
            estimated_cost_micros: e.estimated_cost_micros,
            estimated_arbitrage_savings_micros: e.estimated_arbitrage_savings_micros,
            arbitrage_reason: e.arbitrage_reason.unwrap_or_default(),
            cache_status: e.cache_status,
            route_optimization_mode: e.route_optimization_mode,
            latency_ms: e.latency_ms,
            success: e.success,
            error_message: e.error_message.unwrap_or_default(),
            streaming: e.streaming,
            tool_used: e.tool_used,
            pii_detected: e.pii_detected,
            pii_types: e.pii_types.join(", "),
            policy_action: e.policy_action,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, ToSchema)]
pub struct GatewayRequestsResponse {
    /// Total rows matching the filter (ignoring limit/offset) for pagination.
    pub total: u64,
    pub limit: u32,
    pub offset: u32,
    pub rows: Vec<GatewayRequestRow>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct LlmGatewayRequestEventResponse {
    timestamp: chrono::DateTime<Utc>,
    endpoint_uuid: String,
    provider: String,
    model: String,
    operation: String,
    traffic_source: String,
    consumer_id: Option<String>,
    agent_uuid: Option<String>,
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
    request_bytes: u32,
    response_bytes: u32,
    estimated_cost_micros: u64,
    estimated_arbitrage_savings_micros: u64,
    arbitrage_reason: Option<String>,
    cache_status: String,
    route_optimization_mode: String,
    latency_ms: u64,
    success: bool,
    error_message: Option<String>,
    streaming: bool,
    tool_used: bool,
    policy_action: String,
    pii_detected: bool,
    pii_types: Vec<String>,
}

// ── ClickHouse row shapes ────────────────────────────────────────

#[derive(Debug, Row, Deserialize)]
struct CountRow {
    total: u64,
}

impl From<LlmOperationEventRow> for LlmGatewayRequestEventResponse {
    fn from(value: LlmOperationEventRow) -> Self {
        Self {
            timestamp: value.timestamp,
            endpoint_uuid: value.endpoint_uuid,
            provider: value.provider,
            model: value.model,
            operation: value.operation,
            traffic_source: value.traffic_source,
            consumer_id: non_empty_string(value.consumer_id),
            agent_uuid: non_empty_string(value.agent_uuid),
            prompt_tokens: value.prompt_tokens,
            completion_tokens: value.completion_tokens,
            total_tokens: value.total_tokens,
            request_bytes: value.request_bytes,
            response_bytes: value.response_bytes,
            estimated_cost_micros: value.estimated_cost_micros,
            estimated_arbitrage_savings_micros: value.estimated_arbitrage_savings_micros,
            arbitrage_reason: non_empty_string(value.arbitrage_reason),
            cache_status: value.cache_status,
            route_optimization_mode: value.route_optimization_mode,
            latency_ms: value.latency_ms,
            success: value.success != 0,
            error_message: non_empty_string(value.error_message),
            streaming: value.streaming != 0,
            tool_used: value.tool_used != 0,
            policy_action: value.policy_action,
            pii_detected: value.pii_detected != 0,
            pii_types: value.pii_types,
        }
    }
}

impl From<LlmOperationEvent> for LlmGatewayRequestEventResponse {
    fn from(value: LlmOperationEvent) -> Self {
        Self {
            timestamp: value.timestamp,
            endpoint_uuid: value.endpoint_uuid.uuid().to_string(),
            provider: value.provider,
            model: value.model,
            operation: value.operation,
            traffic_source: value.traffic_source.to_string(),
            consumer_id: value.consumer_id,
            agent_uuid: value.agent_uuid.map(|uuid| uuid.to_string()),
            prompt_tokens: value.prompt_tokens,
            completion_tokens: value.completion_tokens,
            total_tokens: value.total_tokens,
            request_bytes: value.request_bytes,
            response_bytes: value.response_bytes,
            estimated_cost_micros: value.estimated_provider_cost_micros,
            estimated_arbitrage_savings_micros: value.estimated_arbitrage_savings_micros,
            arbitrage_reason: value.arbitrage_reason,
            cache_status: value.cache_status.to_string(),
            route_optimization_mode: value.route_optimization_mode.to_string(),
            latency_ms: value.latency_ms,
            success: value.success,
            error_message: value.error_message,
            streaming: value.streaming,
            tool_used: value.tool_used,
            policy_action: value.policy_action.to_string(),
            pii_detected: value.pii_detected,
            pii_types: value.pii_types,
        }
    }
}

// ── helpers ──────────────────────────────────────────────────────

/// Clamp a requested page size into `[1, MAX_LIMIT]`, defaulting when absent.
fn clamp_limit(raw: Option<&str>) -> u32 {
    raw.and_then(|s| s.trim().parse::<u32>().ok()).map(|n| n.clamp(1, MAX_LIMIT)).unwrap_or(DEFAULT_LIMIT)
}

fn non_empty_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
}

/// Parse a non-negative offset, defaulting to 0.
fn parse_offset(raw: Option<&str>) -> u32 {
    raw.and_then(|s| s.trim().parse::<u32>().ok()).unwrap_or(0)
}

/// Map a `status` param to a SQL predicate fragment over the `success` column.
/// Restricted to a fixed set (no injection); `None` for any other/absent value.
fn status_predicate(raw: Option<&str>) -> Option<&'static str> {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("ok" | "success" | "succeeded") => Some("success = 1"),
        Some("error" | "failed" | "failure") => Some("success = 0"),
        _ => None,
    }
}

/// A set of user-supplied filters compiled to a `WHERE` fragment with `?`
/// placeholders plus the values to bind, in order. User input is bound, never
/// interpolated, so it can't inject SQL.
struct FilterBinds {
    where_sql: String,
    binds: Vec<String>,
}

fn build_filters(params: &HashMap<String, String>) -> FilterBinds {
    let mut clauses: Vec<String> = Vec::new();
    let mut binds: Vec<String> = Vec::new();

    if let Some(v) = optional_param(params, "provider") {
        clauses.push("provider = ?".to_string());
        binds.push(v.to_string());
    }
    if let Some(v) = optional_param(params, "model") {
        clauses.push("model = ?".to_string());
        binds.push(v.to_string());
    }
    if let Some(v) = optional_param(params, "consumer_id") {
        clauses.push("consumer_id = ?".to_string());
        binds.push(v.to_string());
    }
    if let Some(pred) = status_predicate(optional_param(params, "status")) {
        clauses.push(pred.to_string());
    }
    if let Some(v) = optional_param(params, "search") {
        clauses.push("(positionCaseInsensitive(provider, ?) > 0 OR positionCaseInsensitive(model, ?) > 0)".to_string());
        binds.push(v.to_string());
        binds.push(v.to_string());
    }

    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" AND {}", clauses.join(" AND "))
    };
    FilterBinds { where_sql, binds }
}

fn traffic_source_matches(event_source: &str, raw: Option<&str>) -> bool {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("all") => true,
        Some("llm_gateway") => event_source == "llm_gateway",
        Some("agent_gateway") => event_source == "agent_gateway",
        Some("proxy_app") => event_source == "proxy_app",
        Some("internal_job") => event_source == "internal_job",
        None | Some("") | Some(_) => event_source == "proxy_app",
    }
}

fn operation_matches(operation: &str) -> bool {
    matches!(operation, "chat.completions" | "chat.completions.stream" | "responses")
}

fn event_status_matches(success: bool, raw: Option<&str>) -> bool {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("ok" | "success" | "succeeded") => success,
        Some("error" | "failed" | "failure") => !success,
        _ => true,
    }
}

fn text_filter_matches(value: &str, expected: Option<&str>) -> bool {
    expected.is_none_or(|expected| value == expected)
}

fn search_filter_matches(provider: &str, model: &str, raw: Option<&str>) -> bool {
    let Some(raw) = raw else {
        return true;
    };
    let needle = raw.trim();
    if needle.is_empty() {
        return true;
    }
    let needle = needle.to_ascii_lowercase();
    provider.to_ascii_lowercase().contains(&needle) || model.to_ascii_lowercase().contains(&needle)
}

fn recent_gateway_request_rows(
    params: &HashMap<String, String>,
    organization_uuid: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Vec<GatewayRequestRow> {
    let provider = optional_param(params, "provider");
    let model = optional_param(params, "model");
    let consumer_id = optional_param(params, "consumer_id");
    let traffic_source = optional_param(params, "traffic_source");
    let status = optional_param(params, "status");
    let search = optional_param(params, "search");

    recent_llm_operations()
        .into_iter()
        .filter(|event| event.organization_uuid.uuid().to_string() == organization_uuid)
        .filter(|event| event.timestamp >= from && event.timestamp <= to)
        .filter(|event| operation_matches(&event.operation))
        .filter(|event| traffic_source_matches(&event.traffic_source.to_string(), traffic_source))
        .filter(|event| text_filter_matches(&event.provider, provider))
        .filter(|event| text_filter_matches(&event.model, model))
        .filter(|event| text_filter_matches(event.consumer_id.as_deref().unwrap_or_default(), consumer_id))
        .filter(|event| event_status_matches(event.success, status))
        .filter(|event| search_filter_matches(&event.provider, &event.model, search))
        .map(LlmGatewayRequestEventResponse::from)
        .map(GatewayRequestRow::from)
        .collect()
}

fn sort_dedupe_and_limit_rows(mut rows: Vec<GatewayRequestRow>, limit: u32) -> Vec<GatewayRequestRow> {
    rows.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
    let mut seen = BTreeSet::new();
    rows.retain(|row| {
        seen.insert((
            row.timestamp.clone(),
            row.endpoint_uuid.clone(),
            row.operation.clone(),
            row.provider.clone(),
            row.model.clone(),
            row.consumer_id.clone(),
            row.prompt_fingerprint_key(),
        ))
    });
    rows.truncate(limit as usize);
    rows
}

impl GatewayRequestRow {
    fn prompt_fingerprint_key(&self) -> String {
        format!("{}:{}:{}", self.total_tokens, self.latency_ms, self.error_message)
    }
}

// ── handler ──────────────────────────────────────────────────────

/// Paginated, filterable gateway request history.
///
/// **Permissions**: `ControlPerms::READ`.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["LLM"],
    path = "/llm/gateway/requests",
    operation_id = "llm_gateway_requests",
    responses((status = OK, body = GatewayRequestsResponse))
)]
pub async fn gateway_requests(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    query: web::Query<HashMap<String, String>>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let organization_uuid = super::cost::llm_analytics_organization_uuid(auth.org_uuid());
    let params = query.into_inner();

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

    let limit = clamp_limit(optional_param(&params, "limit"));
    let offset = parse_offset(optional_param(&params, "offset"));
    let traffic_filter =
        super::cost::traffic_source_filter(optional_param(&params, "traffic_source"), optional_param(&params, "traffic_class"))?;
    let filters = build_filters(&params);

    let from_str = from.to_rfc3339();
    let to_str = to.to_rfc3339();
    let recent_rows = recent_gateway_request_rows(&params, &organization_uuid, from, to);
    let recent_total = recent_rows.len() as u64;
    let recent_page = recent_rows.into_iter().skip(offset as usize).take(limit as usize).collect::<Vec<_>>();

    // Shared WHERE for both the page query and the count query. The
    // traffic-source / status fragments come from fixed sets (safe to embed);
    // all user-supplied values are bound positionally below.
    let where_clause = format!(
        "WHERE organization_uuid = ? \
         AND timestamp BETWEEN parseDateTime64BestEffort(?) AND parseDateTime64BestEffort(?) \
         AND {traffic_filter}{extra}",
        extra = filters.where_sql,
    );

    let mut total = recent_total;
    let mut rows = Vec::new();
    match database.clickhouse_pool().get().await {
        Ok(client) => {
            // ── total (for pagination) ──
            let count_sql = format!("SELECT count() AS total FROM {table} {where_clause}", table = llm_tables::LLM_OPERATION_EVENTS,);
            let mut count_q = client.query(&count_sql).bind(&organization_uuid).bind(&from_str).bind(&to_str);
            for b in &filters.binds {
                count_q = count_q.bind(b);
            }
            match count_q.fetch_one::<CountRow>().await {
                Ok(count) => total = total.max(count.total),
                Err(error) => {
                    log::warn!("gateway requests count query unavailable; falling back to recent in-memory events: {error}");
                }
            }

            // ── page rows ──
            let rows_sql = format!(
                "SELECT ?fields FROM {table} {where_clause} ORDER BY timestamp DESC LIMIT {limit} OFFSET {offset}",
                table = llm_tables::LLM_OPERATION_EVENTS,
            );
            let mut rows_q = client.query(&rows_sql).bind(&organization_uuid).bind(&from_str).bind(&to_str);
            for b in &filters.binds {
                rows_q = rows_q.bind(b);
            }
            match rows_q.fetch_all::<LlmOperationEventRow>().await {
                Ok(event_rows) => {
                    rows.extend(event_rows.into_iter().map(LlmGatewayRequestEventResponse::from).map(GatewayRequestRow::from));
                }
                Err(error) => {
                    log::warn!("gateway requests query unavailable; falling back to recent in-memory events: {error}");
                }
            }
        }
        Err(error) => {
            log::warn!("LLM request-history analytics backend unavailable; falling back to recent in-memory events: {error}");
        }
    }

    rows.extend(recent_page);
    rows = sort_dedupe_and_limit_rows(rows, limit);
    total = total.max(rows.len() as u64);

    let response = GatewayRequestsResponse { total, limit, offset, rows };
    Ok(HttpResponse::Ok().json(EdenResponse::response(response)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_limit_defaults_and_caps() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some("garbage")), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some("0")), 1);
        assert_eq!(clamp_limit(Some("25")), 25);
        assert_eq!(clamp_limit(Some("9999")), MAX_LIMIT);
    }

    #[test]
    fn parse_offset_defaults_to_zero() {
        assert_eq!(parse_offset(None), 0);
        assert_eq!(parse_offset(Some("garbage")), 0);
        assert_eq!(parse_offset(Some("40")), 40);
    }

    #[test]
    fn status_predicate_fixed_set() {
        assert_eq!(status_predicate(Some("ok")), Some("success = 1"));
        assert_eq!(status_predicate(Some("error")), Some("success = 0"));
        assert_eq!(status_predicate(Some("failed")), Some("success = 0"));
        assert_eq!(status_predicate(Some("whatever")), None);
        assert_eq!(status_predicate(None), None);
    }

    #[test]
    fn build_filters_binds_in_order() {
        let mut params = HashMap::new();
        params.insert("provider".to_string(), "openai".to_string());
        params.insert("model".to_string(), "gpt-4o".to_string());
        params.insert("search".to_string(), "claude".to_string());
        let f = build_filters(&params);
        // provider + model + search(x2) = 4 binds, in declaration order.
        assert_eq!(f.binds, vec!["openai", "gpt-4o", "claude", "claude"]);
        assert!(f.where_sql.contains("provider = ?"));
        assert!(f.where_sql.contains("model = ?"));
        assert!(f.where_sql.contains("positionCaseInsensitive"));
        assert!(f.where_sql.starts_with(" AND "));
    }

    #[test]
    fn build_filters_empty_when_no_params() {
        let params = HashMap::new();
        let f = build_filters(&params);
        assert!(f.where_sql.is_empty());
        assert!(f.binds.is_empty());
    }

    #[test]
    fn traffic_source_matches_defaults_to_proxy_app() {
        assert!(traffic_source_matches("proxy_app", None));
        assert!(traffic_source_matches("proxy_app", Some("")));
        assert!(traffic_source_matches("proxy_app", Some("unknown")));
        assert!(traffic_source_matches("agent_gateway", Some("all")));
        assert!(traffic_source_matches("agent_gateway", Some("agent_gateway")));
        assert!(!traffic_source_matches("agent_gateway", None));
    }

    #[test]
    fn in_memory_filters_match_sql_filter_semantics() {
        assert!(operation_matches("chat.completions"));
        assert!(!operation_matches("embeddings"));
        assert!(event_status_matches(true, Some("ok")));
        assert!(event_status_matches(false, Some("failed")));
        assert!(!event_status_matches(true, Some("error")));
        assert!(search_filter_matches("OpenAI", "synthetic-gpt", Some("open")));
        assert!(search_filter_matches("OpenAI", "synthetic-gpt", Some("GPT")));
        assert!(!search_filter_matches("OpenAI", "synthetic-gpt", Some("claude")));
    }
}
