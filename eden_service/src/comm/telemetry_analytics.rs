//! ClickHouse-backed telemetry export endpoints.

use crate::comm::rbac::verify_control_perms;
use crate::{EdenDb, error_handling};
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use analytics_schema::telemetry::{CountRow, LogRow, MetricExportRow, TraceRow, tables};
use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use database::lib::ClickhousePooledConnection;
use eden_core::auth::ParsedJwt;
use eden_core::format::{EdenUuid, rbac::ControlPerms};
use eden_core::telemetry::{FastSpan, LABEL_TRAFFIC_CLASS, TRAFFIC_CLASS_EXTERNAL, TRAFFIC_CLASS_INTERNAL, TelemetryWrapper};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

const DEFAULT_LIMIT: usize = 500;
const MAX_LIMIT: usize = 5_000;
const MAX_OFFSET: usize = 100_000;
const MAX_SAMPLE_POINTS: usize = 1_000;
const DEFAULT_RANGE_SECS: i64 = 60 * 60;
const MAX_RANGE_SECS: i64 = 365 * 24 * 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signal {
    Metrics,
    Traces,
    Logs,
}

impl Signal {
    fn parse(raw: Option<&str>) -> Result<Self, actix_web::Error> {
        match raw.unwrap_or("metrics").to_ascii_lowercase().as_str() {
            "metric" | "metrics" => Ok(Self::Metrics),
            "trace" | "traces" | "spans" => Ok(Self::Traces),
            "log" | "logs" => Ok(Self::Logs),
            other => Err(actix_web::error::ErrorBadRequest(format!(
                "invalid telemetry signal: {other}; expected metrics, traces, or logs"
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Traces => "traces",
            Self::Logs => "logs",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    fn parse(raw: Option<&str>) -> Result<Self, actix_web::Error> {
        match raw.unwrap_or("desc").to_ascii_lowercase().as_str() {
            "asc" => Ok(Self::Asc),
            "desc" => Ok(Self::Desc),
            other => Err(actix_web::error::ErrorBadRequest(format!("invalid order: {other}; expected asc or desc"))),
        }
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrafficClassFilter {
    All,
    External,
    Internal,
}

impl TrafficClassFilter {
    fn parse(raw: Option<&str>) -> Result<Self, actix_web::Error> {
        match raw.unwrap_or("all").to_ascii_lowercase().as_str() {
            "" | "all" => Ok(Self::All),
            "external" => Ok(Self::External),
            "internal" => Ok(Self::Internal),
            other => Err(actix_web::error::ErrorBadRequest(format!(
                "invalid traffic_class: {other}; expected external, internal, or all"
            ))),
        }
    }

    fn map_condition(self, column: &str) -> Option<String> {
        match self {
            Self::All => None,
            Self::External => Some(format!("{column}['{LABEL_TRAFFIC_CLASS}'] = '{TRAFFIC_CLASS_EXTERNAL}'")),
            Self::Internal => Some(format!("{column}['{LABEL_TRAFFIC_CLASS}'] = '{TRAFFIC_CLASS_INTERNAL}'")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MetricTarget {
    group: &'static str,
    table: &'static str,
}

const METRIC_TARGETS: &[MetricTarget] = &[
    MetricTarget { group: "analytics", table: tables::ANALYTICS },
    MetricTarget { group: "eden", table: tables::EDEN },
    MetricTarget { group: "iam", table: tables::IAM },
    MetricTarget { group: "endpoint", table: tables::ENDPOINT },
    MetricTarget { group: "metadata", table: tables::METADATA },
    MetricTarget { group: "proxy", table: tables::PROXY },
    MetricTarget { group: "snapshot", table: tables::SNAPSHOT },
    MetricTarget { group: "workload", table: tables::WORKLOAD },
];

#[derive(Debug)]
pub(crate) struct ExportRequest {
    signal: Signal,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    order: SortOrder,
    limit: usize,
    offset: usize,
    sample_bucket_ms: Option<i64>,
    filters: BTreeMap<String, String>,
}

#[derive(Serialize, ToSchema)]
#[schema(bound = "T: ToSchema")]
pub(crate) struct TelemetryExportResponse<T> {
    signal: &'static str,
    table: Option<&'static str>,
    group: Option<&'static str>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    order: &'static str,
    limit: usize,
    offset: usize,
    filters: BTreeMap<String, String>,
    total: usize,
    rows: Vec<T>,
}

impl<T> TelemetryExportResponse<T> {
    #[allow(dead_code)]
    pub(crate) fn build(table: Option<&'static str>, group: Option<&'static str>, export: ExportRequest, rows: Vec<T>) -> Self {
        let total = rows.len();

        Self {
            signal: export.signal.as_str(),
            table,
            group,
            from: export.from,
            to: export.to,
            order: export.order.as_str(),
            limit: export.limit,
            offset: export.offset,
            filters: export.filters,
            total,
            rows,
        }
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct MetricRecord {
    group: String,
    timestamp: DateTime<Utc>,
    organization_uuid: String,
    service_name: String,
    node_uuid: String,
    metric_name: String,
    metric_kind: String,
    value: Option<f64>,
    count: Option<u64>,
    sum: Option<f64>,
    bucket_bounds: Vec<f64>,
    bucket_counts: Vec<u64>,
    labels: BTreeMap<String, String>,
    scope: String,
}

#[derive(Serialize)]
struct TraceRecord {
    timestamp: DateTime<Utc>,
    organization_uuid: String,
    service_name: String,
    node_uuid: String,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    span_name: String,
    span_kind: String,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    duration_ns: u64,
    status: String,
    status_message: String,
    attributes: BTreeMap<String, String>,
    events_json: String,
}

#[derive(Serialize)]
struct LogRecord {
    timestamp: DateTime<Utc>,
    service_name: String,
    node_uuid: String,
    level: String,
    audience: String,
    message: String,
    trace_id: String,
    span_id: String,
    feature: String,
    function: String,
    file: String,
    line: Option<u32>,
    eden_node_uuid: String,
    organization_uuid: String,
    organization_id: String,
    user_uuid: String,
    user_id: String,
    endpoint_uuid: String,
    endpoint_id: String,
    endpoint_kind: String,
    error_code: String,
    error_category: String,
    labels: BTreeMap<String, String>,
}

/// GET /api/v1/analytics/telemetry?signal=metrics&group=proxy&range=1h
///
/// Export ClickHouse-backed telemetry rows for metrics, traces, or logs.
#[with_telemetry]
pub async fn export(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    export_inner(req, None, auth, database, telemetry_wrapper, &mut span).await
}

/// GET /api/v1/analytics/telemetry/{signal}?from=...&to=...
///
/// Path-based form for clients that prefer the signal in the URL path.
#[with_telemetry]
pub async fn export_signal(
    req: HttpRequest,
    signal: web::Path<String>,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    export_inner(req, Some(signal.into_inner()), auth, database, telemetry_wrapper, &mut span).await
}

async fn export_inner(
    req: HttpRequest,
    path_signal: Option<String>,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    telemetry_wrapper: &mut TelemetryWrapper,
    span: &mut FastSpan,
) -> Result<HttpResponse, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, span))?;
    let organization_uuid = auth.org_uuid().uuid().to_string();

    let params = web::Query::<HashMap<String, String>>::from_query(req.query_string())
        .map_err(|err| actix_web::error::ErrorBadRequest(format!("invalid query string: {err}")))?
        .into_inner();
    let export = parse_export_request(path_signal.as_deref(), &params)?;

    let client = database
        .clickhouse_pool()
        .get()
        .await
        .map_err(|_| actix_web::error::ErrorServiceUnavailable("analytics backend unavailable"))?;

    match export.signal {
        Signal::Metrics => {
            let group = optional_param(&params, "group");
            let targets = metric_targets(group)?;
            let where_clause = build_where_clause(&export, &organization_uuid, metric_conditions(&params)?);
            let sql = metric_rows_sql(&targets, &where_clause, &export);
            let count_sql = metric_count_sql(&targets, &where_clause);
            let total = fetch_total(&client, &count_sql, "metric").await?;
            let rows: Vec<MetricExportRow> = client.query(&sql).fetch_all().await.map_err(|err| {
                log::error!("failed to export ClickHouse metric telemetry: {err}");
                actix_web::error::ErrorInternalServerError("failed to export metric telemetry")
            })?;
            let rows = rows.into_iter().map(MetricRecord::from_export_row).collect();
            let selected_target = (targets.len() == 1).then_some(targets[0]);
            Ok(HttpResponse::Ok().json(TelemetryExportResponse {
                signal: export.signal.as_str(),
                table: selected_target.map(|target| target.table),
                group: selected_target.map(|target| target.group),
                from: export.from,
                to: export.to,
                order: export.order.as_str(),
                limit: export.limit,
                offset: export.offset,
                filters: export.filters,
                total,
                rows,
            }))
        }
        Signal::Traces => {
            let mut where_clause = build_where_clause(&export, &organization_uuid, trace_conditions(&params)?);
            if let Some(origin_clause) = trace_origin_filter_clause(&export, &organization_uuid, &params)? {
                where_clause.push_str(" AND ");
                where_clause.push_str(&origin_clause);
            }
            let sql = format!(
                r#"
                SELECT
                    timestamp,
                    organization_uuid,
                    service_name,
                    node_uuid,
                    trace_id,
                    span_id,
                    parent_span_id,
                    span_name,
                    span_kind,
                    start_time,
                    end_time,
                    duration_ns,
                    status,
                    status_message,
                    attributes,
                    events_json
                FROM {table}
                WHERE {where_clause}
                {order_limit_offset}
                "#,
                table = tables::TRACES,
                order_limit_offset = order_limit_offset_clause(&export, "timestamp"),
            );
            let count_sql = count_sql(tables::TRACES, &where_clause);

            let rows: Vec<TraceRow> = match client.query(&sql).fetch_all().await {
                Ok(rows) => rows,
                Err(err) if clickhouse_table_missing(&err) => Vec::new(),
                Err(err) => {
                    log::error!("failed to export ClickHouse trace telemetry: {err}");
                    return Err(actix_web::error::ErrorInternalServerError("failed to export trace telemetry"));
                }
            };
            let rows: Vec<_> = rows.into_iter().map(TraceRecord::from_row).collect();
            let total = match fetch_total(&client, &count_sql, "trace").await {
                Ok(total) => total,
                Err(err) if rows.is_empty() => {
                    log::warn!("returning empty trace telemetry export because backing table is unavailable: {err}");
                    0
                }
                Err(err) => return Err(err),
            };
            Ok(HttpResponse::Ok().json(TelemetryExportResponse {
                signal: export.signal.as_str(),
                table: Some(tables::TRACES),
                group: None,
                from: export.from,
                to: export.to,
                order: export.order.as_str(),
                limit: export.limit,
                offset: export.offset,
                filters: export.filters,
                total,
                rows,
            }))
        }
        Signal::Logs => {
            let where_clause = build_where_clause(&export, &organization_uuid, log_conditions(&params)?);
            let sql = format!(
                r#"
                SELECT
                    timestamp,
                    service_name,
                    node_uuid,
                    level,
                    audience,
                    message,
                    trace_id,
                    span_id,
                    feature,
                    function,
                    file,
                    line,
                    eden_node_uuid,
                    organization_uuid,
                    organization_id,
                    user_uuid,
                    user_id,
                    endpoint_uuid,
                    endpoint_id,
                    endpoint_kind,
                    error_code,
                    error_category,
                    labels
                FROM {table}
                WHERE {where_clause}
                {order_limit_offset}
                "#,
                table = tables::LOGS,
                order_limit_offset = order_limit_offset_clause(&export, "timestamp"),
            );
            let count_sql = count_sql(tables::LOGS, &where_clause);

            let rows: Vec<LogRow> = match client.query(&sql).fetch_all().await {
                Ok(rows) => rows,
                Err(err) if clickhouse_table_missing(&err) => Vec::new(),
                Err(err) => {
                    log::error!("failed to export ClickHouse log telemetry: {err}");
                    return Err(actix_web::error::ErrorInternalServerError("failed to export log telemetry"));
                }
            };
            let rows: Vec<_> = rows.into_iter().map(LogRecord::from_row).collect();
            let total = match fetch_total(&client, &count_sql, "log").await {
                Ok(total) => total,
                Err(err) if rows.is_empty() => {
                    log::warn!("returning empty log telemetry export because backing table is unavailable: {err}");
                    0
                }
                Err(err) => return Err(err),
            };
            Ok(HttpResponse::Ok().json(TelemetryExportResponse {
                signal: export.signal.as_str(),
                table: Some(tables::LOGS),
                group: None,
                from: export.from,
                to: export.to,
                order: export.order.as_str(),
                limit: export.limit,
                offset: export.offset,
                filters: export.filters,
                total,
                rows,
            }))
        }
    }
}

fn clickhouse_table_missing(err: &impl std::fmt::Display) -> bool {
    let message = err.to_string();
    message.contains("UNKNOWN_TABLE") || message.contains("Unknown table expression")
}

impl MetricRecord {
    fn from_export_row(row: MetricExportRow) -> Self {
        Self {
            group: row.metric_group,
            timestamp: row.timestamp,
            organization_uuid: row.organization_uuid,
            service_name: row.service_name,
            node_uuid: row.node_uuid,
            metric_name: row.metric_name,
            metric_kind: row.metric_kind,
            value: row.value,
            count: row.count,
            sum: row.sum,
            bucket_bounds: row.bucket_bounds,
            bucket_counts: row.bucket_counts,
            labels: pairs_to_map(row.labels),
            scope: row.scope,
        }
    }
}

async fn fetch_total(client: &ClickhousePooledConnection, sql: &str, signal: &str) -> Result<usize, actix_web::Error> {
    let row: CountRow = client.query(sql).fetch_one().await.map_err(|err| {
        log::error!("failed to count ClickHouse {signal} telemetry: {err}");
        actix_web::error::ErrorInternalServerError(format!("failed to count {signal} telemetry"))
    })?;
    Ok(row.total.min(usize::MAX as u64) as usize)
}

impl TraceRecord {
    fn from_row(row: TraceRow) -> Self {
        Self {
            timestamp: row.timestamp,
            organization_uuid: row.organization_uuid,
            service_name: row.service_name,
            node_uuid: row.node_uuid,
            trace_id: row.trace_id,
            span_id: row.span_id,
            parent_span_id: row.parent_span_id,
            span_name: row.span_name,
            span_kind: row.span_kind,
            start_time: row.start_time,
            end_time: row.end_time,
            duration_ns: row.duration_ns,
            status: row.status,
            status_message: row.status_message,
            attributes: pairs_to_map(row.attributes),
            events_json: row.events_json,
        }
    }
}

impl LogRecord {
    fn from_row(row: LogRow) -> Self {
        Self {
            timestamp: row.timestamp,
            service_name: row.service_name,
            node_uuid: row.node_uuid,
            level: row.level,
            audience: row.audience,
            message: row.message,
            trace_id: row.trace_id,
            span_id: row.span_id,
            feature: row.feature,
            function: row.function,
            file: row.file,
            line: row.line,
            eden_node_uuid: row.eden_node_uuid,
            organization_uuid: row.organization_uuid,
            organization_id: row.organization_id,
            user_uuid: row.user_uuid,
            user_id: row.user_id,
            endpoint_uuid: row.endpoint_uuid,
            endpoint_id: row.endpoint_id,
            endpoint_kind: row.endpoint_kind,
            error_code: row.error_code,
            error_category: row.error_category,
            labels: pairs_to_map(row.labels),
        }
    }
}

pub(crate) fn parse_export_request(path_signal: Option<&str>, params: &HashMap<String, String>) -> Result<ExportRequest, actix_web::Error> {
    let signal = Signal::parse(path_signal.or_else(|| optional_param(params, "signal")))?;
    let order = SortOrder::parse(optional_param(params, "order"))?;
    let limit = parse_usize(params, "limit")?.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = parse_usize(params, "offset")?.unwrap_or(0).min(MAX_OFFSET);
    let sample_points = parse_usize(params, "sample_points")?.map(|value| value.clamp(1, MAX_SAMPLE_POINTS));
    let to = parse_optional_time(params, "to")?.unwrap_or_else(Utc::now);
    let from = match parse_optional_time(params, "from")? {
        Some(from) => from,
        None => {
            let range = parse_range_secs(optional_param(params, "range"))?.unwrap_or(DEFAULT_RANGE_SECS);
            to - Duration::seconds(range)
        }
    };

    if from > to {
        return Err(actix_web::error::ErrorBadRequest("from must be before or equal to to"));
    }

    let sample_bucket_ms = (signal == Signal::Metrics).then_some(sample_points).flatten().and_then(|points| {
        let duration_ms = (to - from).num_milliseconds();
        (duration_ms > 0).then(|| ((duration_ms + points as i64 - 1) / points as i64).max(1))
    });

    Ok(ExportRequest {
        signal,
        from,
        to,
        order,
        limit,
        offset,
        sample_bucket_ms,
        filters: accepted_filters(params),
    })
}

fn build_where_clause(export: &ExportRequest, organization_uuid: &str, mut conditions: Vec<String>) -> String {
    conditions.insert(
        0,
        format!(
            "timestamp >= toDateTime64('{}', 3, 'UTC') AND timestamp <= toDateTime64('{}', 3, 'UTC')",
            clickhouse_time(export.from),
            clickhouse_time(export.to)
        ),
    );
    conditions.insert(1, format!("organization_uuid = '{}'", escape_clickhouse_string(organization_uuid)));
    conditions.join(" AND ")
}

fn metric_rows_sql(targets: &[MetricTarget], where_clause: &str, export: &ExportRequest) -> String {
    let union = targets.iter().map(|target| metric_select_sql(*target, where_clause)).collect::<Vec<_>>().join("\nUNION ALL\n");
    format!(
        r#"
        SELECT *
        FROM (
            {union}
        ) AS metric_rows
        {metric_order_limit_offset}
        "#,
        metric_order_limit_offset = metric_order_limit_offset_clause(export),
    )
}

fn metric_select_sql(target: MetricTarget, where_clause: &str) -> String {
    format!(
        r#"
        SELECT
            '{group}' AS metric_group,
            timestamp,
            organization_uuid,
            service_name,
            node_uuid,
            metric_name,
            metric_kind,
            value,
            count,
            sum,
            bucket_bounds,
            bucket_counts,
            labels,
            scope
        FROM {table}
        WHERE {where_clause}
        "#,
        group = target.group,
        table = target.table,
    )
}

fn metric_count_sql(targets: &[MetricTarget], where_clause: &str) -> String {
    let union = targets
        .iter()
        .map(|target| format!("SELECT count() AS row_count FROM {} WHERE {}", target.table, where_clause))
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n");
    format!(
        r#"
        SELECT toUInt64(sum(row_count)) AS total
        FROM (
            {union}
        ) AS metric_counts
        "#
    )
}

fn count_sql(table: &str, where_clause: &str) -> String {
    format!("SELECT count() AS total FROM {table} WHERE {where_clause}")
}

fn order_limit_offset_clause(export: &ExportRequest, order_columns: &str) -> String {
    format!(
        "ORDER BY {order_columns} {order} LIMIT {limit} OFFSET {offset}",
        order = export.order.sql(),
        limit = export.limit,
        offset = export.offset,
    )
}

fn metric_order_limit_offset_clause(export: &ExportRequest) -> String {
    let limit_by = export.sample_bucket_ms.map(|bucket_ms| {
        format!(
            " LIMIT 1 BY metric_group, metric_name, node_uuid, cityHash64(labels), intDiv(toInt64(toUnixTimestamp64Milli(timestamp)) - {range_start_ms}, {bucket_ms})",
            range_start_ms = export.from.timestamp_millis(),
        )
    }).unwrap_or_default();
    format!(
        "ORDER BY timestamp {order}, metric_group ASC, metric_name ASC, node_uuid ASC{limit_by} LIMIT {limit} OFFSET {offset}",
        order = export.order.sql(),
        limit_by = limit_by,
        limit = export.limit,
        offset = export.offset,
    )
}

pub(crate) fn metric_conditions(params: &HashMap<String, String>) -> Result<Vec<String>, actix_web::Error> {
    let mut conditions = common_conditions(params, &["service_name", "node_uuid", "scope"])?;
    add_metric_name_filter(params, &mut conditions)?;
    add_string_filter(params, &mut conditions, "metric_kind")?;
    add_traffic_class_filter(params, &mut conditions, "labels")?;
    add_map_filters(params, &mut conditions, &["label.", "labels.", "tag.", "tags."], "labels")?;
    Ok(conditions)
}

fn trace_conditions(params: &HashMap<String, String>) -> Result<Vec<String>, actix_web::Error> {
    let mut conditions = common_conditions(params, &["service_name", "node_uuid", "trace_id", "span_id", "parent_span_id"])?;
    add_string_filter(params, &mut conditions, "span_name")?;
    add_string_filter(params, &mut conditions, "span_kind")?;
    add_string_filter(params, &mut conditions, "status")?;
    add_map_filters(
        params,
        &mut conditions,
        &["attr.", "attrs.", "attribute.", "attributes.", "tag.", "tags."],
        "attributes",
    )?;
    Ok(conditions)
}

fn trace_origin_filter_clause(
    export: &ExportRequest,
    organization_uuid: &str,
    params: &HashMap<String, String>,
) -> Result<Option<String>, actix_web::Error> {
    let Some(origin_condition) = TrafficClassFilter::parse(optional_param(params, "traffic_class"))?.map_condition("attributes") else {
        return Ok(None);
    };
    Ok(Some(format!(
        "trace_id IN (SELECT trace_id FROM {table} WHERE timestamp >= toDateTime64('{from}', 3, 'UTC') AND timestamp <= toDateTime64('{to}', 3, 'UTC') AND organization_uuid = '{org}' AND {origin_condition})",
        table = tables::TRACES,
        from = clickhouse_time(export.from),
        to = clickhouse_time(export.to),
        org = escape_clickhouse_string(organization_uuid),
    )))
}

fn log_conditions(params: &HashMap<String, String>) -> Result<Vec<String>, actix_web::Error> {
    let mut conditions = common_conditions(
        params,
        &[
            "service_name",
            "node_uuid",
            "trace_id",
            "span_id",
            "level",
            "audience",
            "feature",
            "organization_id",
            "user_uuid",
            "user_id",
            "endpoint_uuid",
            "endpoint_kind",
            "error_code",
            "error_category",
        ],
    )?;
    if let Some(needle) = optional_param(params, "message_contains") {
        conditions.push(format!("positionCaseInsensitive(message, '{}') > 0", escape_clickhouse_string(needle)));
    }
    add_traffic_class_filter(params, &mut conditions, "labels")?;
    add_map_filters(params, &mut conditions, &["label.", "labels.", "tag.", "tags."], "labels")?;
    Ok(conditions)
}

fn common_conditions(params: &HashMap<String, String>, keys: &[&str]) -> Result<Vec<String>, actix_web::Error> {
    let mut conditions = Vec::new();
    for key in keys {
        add_string_filter(params, &mut conditions, key)?;
    }
    Ok(conditions)
}

fn add_string_filter(params: &HashMap<String, String>, conditions: &mut Vec<String>, key: &str) -> Result<(), actix_web::Error> {
    if let Some(value) = optional_param(params, key) {
        validate_identifier(key)?;
        conditions.push(format!("{key} = '{}'", escape_clickhouse_string(value)));
    }
    Ok(())
}

fn add_metric_name_filter(params: &HashMap<String, String>, conditions: &mut Vec<String>) -> Result<(), actix_web::Error> {
    let Some(value) = optional_param(params, "metric_name") else {
        return Ok(());
    };
    validate_identifier("metric_name")?;
    let candidates = metric_name_candidates(value);
    if candidates.len() == 1 {
        conditions.push(format!("metric_name = '{}'", escape_clickhouse_string(&candidates[0])));
    } else {
        let names = candidates.iter().map(|candidate| format!("'{}'", escape_clickhouse_string(candidate))).collect::<Vec<_>>().join(", ");
        conditions.push(format!("metric_name IN ({names})"));
    }
    Ok(())
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
    if let Some(rest) = metric_name.strip_prefix("gateway.") {
        return Some(format!("gateway_{rest}"));
    }
    None
}

fn add_traffic_class_filter(
    params: &HashMap<String, String>,
    conditions: &mut Vec<String>,
    map_column: &str,
) -> Result<(), actix_web::Error> {
    if let Some(condition) = TrafficClassFilter::parse(optional_param(params, "traffic_class"))?.map_condition(map_column) {
        conditions.push(condition);
    }
    Ok(())
}

fn add_map_filters(
    params: &HashMap<String, String>,
    conditions: &mut Vec<String>,
    prefixes: &[&str],
    column: &str,
) -> Result<(), actix_web::Error> {
    for (key, value) in params {
        if let Some(map_key) = prefixes.iter().find_map(|prefix| key.strip_prefix(prefix)) {
            validate_map_key(map_key)?;
            conditions.push(format!("{column}['{}'] = '{}'", escape_clickhouse_string(map_key), escape_clickhouse_string(value)));
        }
    }
    Ok(())
}

fn metric_targets(group: Option<&str>) -> Result<Vec<MetricTarget>, actix_web::Error> {
    match group {
        None | Some("") | Some("all") => Ok(METRIC_TARGETS.to_vec()),
        Some(raw) => {
            let normalized = raw.to_ascii_lowercase();
            METRIC_TARGETS
                .iter()
                .copied()
                .find(|target| target.group == normalized)
                .map(|target| vec![target])
                .ok_or_else(|| actix_web::error::ErrorBadRequest(format!("invalid metric group: {raw}")))
        }
    }
}

pub(crate) fn optional_param<'a>(params: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    params.get(key).map(String::as_str).filter(|value| !value.is_empty())
}

fn parse_usize(params: &HashMap<String, String>, key: &str) -> Result<Option<usize>, actix_web::Error> {
    optional_param(params, key)
        .map(|value| value.parse::<usize>().map_err(|_| actix_web::error::ErrorBadRequest(format!("invalid {key}: {value}"))))
        .transpose()
}

pub(crate) fn parse_optional_time(params: &HashMap<String, String>, key: &str) -> Result<Option<DateTime<Utc>>, actix_web::Error> {
    optional_param(params, key).map(parse_time).transpose()
}

pub(crate) fn parse_time(raw: &str) -> Result<DateTime<Utc>, actix_web::Error> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S").map(|dt| Utc.from_utc_datetime(&dt)))
        .map_err(|_| actix_web::error::ErrorBadRequest(format!("invalid timestamp: {raw}; use RFC3339 or YYYY-MM-DD HH:MM:SS")))
}

pub(crate) fn parse_range_secs(raw: Option<&str>) -> Result<Option<i64>, actix_web::Error> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let mut total = 0i64;
    let mut digits = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            continue;
        }
        let value = digits.parse::<i64>().map_err(|_| actix_web::error::ErrorBadRequest(format!("invalid range: {raw}")))?;
        digits.clear();
        let multiplier = match ch {
            's' | 'S' => 1,
            'm' | 'M' => 60,
            'h' | 'H' => 60 * 60,
            'd' | 'D' => 24 * 60 * 60,
            'w' | 'W' => 7 * 24 * 60 * 60,
            _ => return Err(actix_web::error::ErrorBadRequest(format!("invalid range unit in {raw}; use s, m, h, d, or w"))),
        };
        total = total.saturating_add(value.saturating_mul(multiplier));
    }
    if !digits.is_empty() {
        return Err(actix_web::error::ErrorBadRequest(format!("invalid range: {raw}; missing unit")));
    }
    if total <= 0 || total > MAX_RANGE_SECS {
        return Err(actix_web::error::ErrorBadRequest("range must be between 1s and 365d"));
    }
    Ok(Some(total))
}

pub(crate) fn clickhouse_time(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

pub(crate) fn escape_clickhouse_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

pub(crate) fn validate_identifier(identifier: &str) -> Result<(), actix_web::Error> {
    if identifier.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        Ok(())
    } else {
        Err(actix_web::error::ErrorBadRequest(format!("invalid filter name: {identifier}")))
    }
}

pub(crate) fn validate_map_key(key: &str) -> Result<(), actix_web::Error> {
    if !key.is_empty() && key.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':' | b'/')) {
        Ok(())
    } else {
        Err(actix_web::error::ErrorBadRequest(format!("invalid map filter key: {key}")))
    }
}

fn accepted_filters(params: &HashMap<String, String>) -> BTreeMap<String, String> {
    params
        .iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "signal" | "from" | "to" | "range" | "order" | "limit" | "offset" | "sample_points" | "organization_uuid"
            )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn pairs_to_map(pairs: Vec<(String, String)>) -> BTreeMap<String, String> {
    pairs.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_targets_default_to_all_groups() {
        let targets = metric_targets(None).expect("targets");
        assert_eq!(targets.len(), 10);
        assert_eq!(targets[0].table, "analytics.analytics");
        assert_eq!(targets[6].table, "analytics.proxy");
    }

    #[test]
    fn parse_export_request_defaults_to_recent_metrics() {
        let params = HashMap::new();
        let request = parse_export_request(None, &params).expect("request");
        assert_eq!(request.signal, Signal::Metrics);
        assert_eq!(request.limit, DEFAULT_LIMIT);
        assert_eq!(request.order, SortOrder::Desc);
        assert!(request.to >= request.from);
    }

    #[test]
    fn metric_rows_query_paginates_once_after_unioning_targets() {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "25".to_string());
        params.insert("offset".to_string(), "100".to_string());
        params.insert("order".to_string(), "asc".to_string());
        let export = parse_export_request(None, &params).expect("request");
        let targets = metric_targets(None).expect("targets");

        let sql = metric_rows_sql(&targets, "organization_uuid = 'authorized-org'", &export);

        assert_eq!(sql.matches("UNION ALL").count(), targets.len() - 1);
        assert!(sql.contains("'proxy' AS metric_group"));
        assert!(sql.contains("FROM analytics.proxy"));
        assert!(sql.contains("ORDER BY timestamp ASC, metric_group ASC, metric_name ASC, node_uuid ASC LIMIT 25 OFFSET 100"));
        assert!(!sql.contains("LIMIT 125"));
    }

    #[test]
    fn metric_rows_query_samples_across_the_requested_window() {
        let mut params = HashMap::new();
        params.insert("from".to_string(), "2026-05-05T00:00:00Z".to_string());
        params.insert("to".to_string(), "2026-05-06T00:00:00Z".to_string());
        params.insert("sample_points".to_string(), "48".to_string());
        let export = parse_export_request(None, &params).expect("request");
        let targets = metric_targets(Some("proxy")).expect("targets");

        let sql = metric_rows_sql(&targets, "organization_uuid = 'authorized-org'", &export);

        assert_eq!(export.sample_bucket_ms, Some(30 * 60 * 1_000));
        assert!(sql.contains("LIMIT 1 BY metric_group, metric_name, node_uuid, cityHash64(labels)"));
        assert!(sql.contains("1800000"));
        assert!(sql.contains("LIMIT 500 OFFSET 0"));
        assert!(!export.filters.contains_key("sample_points"));
    }

    #[test]
    fn count_queries_do_not_include_page_bounds() {
        let targets = metric_targets(Some("proxy")).expect("targets");
        let metric_sql = metric_count_sql(&targets, "organization_uuid = 'authorized-org'");
        let log_sql = count_sql(tables::LOGS, "organization_uuid = 'authorized-org'");

        assert!(metric_sql.contains("SELECT toUInt64(sum(row_count)) AS total"));
        assert!(metric_sql.contains("SELECT count() AS row_count FROM analytics.proxy"));
        assert!(log_sql.contains("SELECT count() AS total FROM analytics.logs"));
        assert!(!metric_sql.contains("LIMIT"));
        assert!(!log_sql.contains("OFFSET"));
    }

    #[test]
    fn map_filter_rejects_unsafe_keys() {
        let mut params = HashMap::new();
        params.insert("label.good_key".to_string(), "ok".to_string());
        params.insert("label.bad'key".to_string(), "nope".to_string());

        assert!(metric_conditions(&params).is_err());
    }

    #[test]
    fn where_clause_includes_time_and_filters() {
        let mut params = HashMap::new();
        params.insert("metric_name".to_string(), "gateway.requests_total".to_string());
        params.insert("label.endpoint_uuid".to_string(), "endpoint-1".to_string());
        let export = parse_export_request(None, &params).expect("request");

        let where_clause = build_where_clause(&export, "test", metric_conditions(&params).expect("conditions"));

        assert!(where_clause.contains("timestamp >="));
        assert!(where_clause.contains("organization_uuid = 'test'"));
        assert!(where_clause.contains("metric_name IN ('gateway.requests_total', 'gateway_requests_total')"));
        assert!(where_clause.contains("labels['endpoint_uuid'] = 'endpoint-1'"));
    }

    #[test]
    fn metric_conditions_include_gateway_redis_legacy_aliases() {
        let mut params = HashMap::new();
        params.insert("metric_name".to_string(), "gateway.redis.command_end_to_end_microseconds".to_string());

        let conditions = metric_conditions(&params).expect("conditions").join(" AND ");

        assert!(conditions.contains("'gateway.redis.command_end_to_end_microseconds'"));
        assert!(conditions.contains("'gateway_redis_command_end_to_end_microseconds'"));
        assert!(conditions.contains("'gateway.command_end_to_end_microseconds'"));
        assert!(conditions.contains("'gateway_command_end_to_end_microseconds'"));
    }

    #[test]
    fn caller_supplied_organization_uuid_does_not_override_tenant_scope() {
        let mut params = HashMap::new();
        params.insert("organization_uuid".to_string(), "attacker-org".to_string());
        params.insert("level".to_string(), "ERROR".to_string());
        let export = parse_export_request(Some("logs"), &params).expect("request");

        let where_clause = build_where_clause(&export, "authorized-org", log_conditions(&params).expect("conditions"));

        assert!(where_clause.contains("organization_uuid = 'authorized-org'"));
        assert!(!where_clause.contains("organization_uuid = 'attacker-org'"));
        assert!(where_clause.contains("level = 'ERROR'"));
        assert!(!accepted_filters(&params).contains_key("organization_uuid"));
    }

    #[test]
    fn metric_conditions_accept_tag_aliases() {
        let mut params = HashMap::new();
        params.insert("tag.interlay_uuid".to_string(), "interlay-1".to_string());
        params.insert("tags.shard_id".to_string(), "0".to_string());

        let conditions = metric_conditions(&params).expect("conditions").join(" AND ");

        assert!(conditions.contains("labels['interlay_uuid'] = 'interlay-1'"));
        assert!(conditions.contains("labels['shard_id'] = '0'"));
    }

    #[test]
    fn trace_conditions_accept_tag_aliases() {
        let mut params = HashMap::new();
        params.insert("tag.endpoint_uuid".to_string(), "endpoint-1".to_string());

        let conditions = trace_conditions(&params).expect("conditions").join(" AND ");

        assert!(conditions.contains("attributes['endpoint_uuid'] = 'endpoint-1'"));
    }
}
