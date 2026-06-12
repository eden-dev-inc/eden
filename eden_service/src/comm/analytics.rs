//! Analytics HTTP endpoints for status and control.
use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpResponse, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::format::rbac::ControlPerms;
use eden_core::telemetry::AllMetrics;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use telemetry_extensions_macro::with_telemetry;

/// The verbose capture pipeline has been removed from this build.
const VERBOSE_PIPELINE_COMPILED: bool = false;

#[derive(Serialize)]
struct AnalyticsStatusResponse {
    /// Whether the verbose capture pipeline is currently recording.
    /// `false` when the feature is compiled but the runtime toggle is off,
    /// or when the feature is not compiled at all.
    enabled: bool,
    /// Whether the verbose capture pipeline exists in this build.
    /// The high-level analytics overview (`/analytics/overview`) and live
    /// connection metrics are available regardless of this flag.
    verbose_pipeline_compiled: bool,
    health: Option<serde_json::Value>,
    schema: AnalyticsSchemaStatusResponse,
}

#[derive(Serialize)]
struct AnalyticsSchemaStatusResponse {
    ok: bool,
    checked_at: String,
    error: Option<String>,
    tables: Vec<AnalyticsSchemaTableStatus>,
}

#[derive(Serialize)]
struct AnalyticsSchemaTableStatus {
    table: String,
    present: bool,
    column_count: usize,
    missing_columns: Vec<String>,
    legacy_columns: Vec<String>,
}

#[derive(clickhouse::Row, Deserialize)]
struct AnalyticsSchemaColumnRow {
    table_name: String,
    column_name: String,
}

struct ExpectedAnalyticsTable {
    name: &'static str,
    columns: &'static [&'static str],
}

const EXPECTED_ANALYTICS_TABLES: &[ExpectedAnalyticsTable] = &[
    ExpectedAnalyticsTable {
        name: "command_rollups",
        columns: &[
            "organization_uuid",
            "endpoint_uuid",
            "protocol",
            "command",
            "request_count",
            "error_count",
        ],
    },
    ExpectedAnalyticsTable {
        name: "command_rollups_hourly",
        columns: &[
            "organization_uuid",
            "endpoint_uuid",
            "protocol",
            "command",
            "request_count",
            "error_count",
        ],
    },
    ExpectedAnalyticsTable {
        name: "target_pattern_rollups",
        columns: &[
            "organization_uuid",
            "endpoint_uuid",
            "protocol",
            "target_pattern",
            "request_count",
            "error_count",
        ],
    },
    ExpectedAnalyticsTable {
        name: "endpoint_metrics",
        columns: &["organization_uuid", "endpoint_uuid", "protocol", "total_commands", "total_errors"],
    },
    ExpectedAnalyticsTable {
        name: "llm_operation_rollups",
        columns: &[
            "organization_uuid",
            "endpoint_uuid",
            "provider",
            "model",
            "request_count",
            "total_tokens_sum",
        ],
    },
    ExpectedAnalyticsTable {
        name: "llm_operation_events",
        columns: &[
            "organization_uuid",
            "endpoint_uuid",
            "provider",
            "model",
            "timestamp",
            "total_tokens",
        ],
    },
];

const LEGACY_ANALYTICS_COLUMNS: &[&str] = &["tenant_id"];

/// Get analytics availability and pipeline health when verbose capture exists.
///
/// This handler always succeeds: callers use `verbose_pipeline_compiled` plus
/// `enabled` to decide whether to fetch verbose data or fall back to
/// `/analytics/overview`.
///
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
pub async fn status(auth: web::ReqData<ParsedJwt>, database_manager: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database_manager, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let (enabled, health) = collect_status().await;

    Ok(HttpResponse::Ok().json(AnalyticsStatusResponse {
        enabled,
        verbose_pipeline_compiled: VERBOSE_PIPELINE_COMPILED,
        health,
        schema: collect_schema_status(&database_manager).await,
    }))
}

async fn collect_schema_status(database_manager: &EdenDb) -> AnalyticsSchemaStatusResponse {
    let checked_at = chrono::Utc::now().to_rfc3339();
    let query = format!(
        "SELECT table AS table_name, name AS column_name \
         FROM system.columns \
         WHERE database = 'analytics' AND table IN ({}) \
         ORDER BY table, position",
        EXPECTED_ANALYTICS_TABLES.iter().map(|table| format!("'{}'", table.name)).collect::<Vec<_>>().join(",")
    );

    let rows = match database_manager.clickhouse_pool().get().await {
        Ok(client) => match client.query(&query).fetch_all::<AnalyticsSchemaColumnRow>().await {
            Ok(rows) => rows,
            Err(error) => {
                return build_schema_status(checked_at, Vec::new(), Some(format!("failed to inspect analytics schema: {error}")));
            }
        },
        Err(error) => {
            return build_schema_status(checked_at, Vec::new(), Some(format!("analytics backend unavailable: {error}")));
        }
    };

    build_schema_status(checked_at, rows, None)
}

fn build_schema_status(checked_at: String, rows: Vec<AnalyticsSchemaColumnRow>, error: Option<String>) -> AnalyticsSchemaStatusResponse {
    let mut columns_by_table: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for row in rows {
        columns_by_table.entry(row.table_name).or_default().insert(row.column_name);
    }

    let tables = EXPECTED_ANALYTICS_TABLES
        .iter()
        .map(|expected| {
            let columns = columns_by_table.get(expected.name);
            let present = columns.is_some();
            let missing_columns = expected
                .columns
                .iter()
                .filter(|column| match columns {
                    Some(set) => !set.iter().any(|existing| existing == *column),
                    None => true,
                })
                .map(|column| (*column).to_string())
                .collect::<Vec<_>>();
            let legacy_columns = columns
                .map(|set| {
                    LEGACY_ANALYTICS_COLUMNS
                        .iter()
                        .filter(|column| set.iter().any(|existing| existing == *column))
                        .map(|column| (*column).to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            AnalyticsSchemaTableStatus {
                table: format!("analytics.{}", expected.name),
                present,
                column_count: columns.map_or(0, BTreeSet::len),
                missing_columns,
                legacy_columns,
            }
        })
        .collect::<Vec<_>>();

    let ok =
        error.is_none() && tables.iter().all(|table| table.present && table.missing_columns.is_empty() && table.legacy_columns.is_empty());
    AnalyticsSchemaStatusResponse { ok, checked_at, error, tables }
}

async fn collect_status() -> (bool, Option<serde_json::Value>) {
    (false, None)
}

pub async fn enable() -> impl Responder {
    HttpResponse::ServiceUnavailable().body("analytics disabled")
}

pub async fn disable() -> impl Responder {
    HttpResponse::ServiceUnavailable().body("analytics disabled")
}

pub async fn dashboard() -> impl Responder {
    HttpResponse::ServiceUnavailable().body("analytics disabled")
}

// Overview endpoint — high-level fleet metrics from the in-process telemetry registry.
/// Fleet-wide totals (sum across all label sets).
#[derive(Serialize)]
struct FleetTotals {
    total_requests: u64,
    active_requests: i64,
    success_count: u64,
    error_count: u64,
    error_rate: f64,
    upload_bytes: u64,
    download_bytes: u64,
    open_connections: i64,
    in_use_connections: i64,
}

/// Per-endpoint-type aggregate (Mongo / Redis / Postgres / etc.).
#[derive(Serialize)]
struct PerTypeRow {
    endpoint_type: String,
    request_count: u64,
    active_requests: i64,
    error_count: u64,
    upload_bytes: u64,
    download_bytes: u64,
    open_connections: i64,
    in_use_connections: i64,
}

/// Per-endpoint-instance live snapshot. Connection counts come from the stable
/// `ConnectionState` registry keyed by `endpoint_uuid`.
#[derive(Serialize)]
struct PerEndpointRow {
    endpoint_uuid: String,
    endpoint_type: Option<String>,
    open_connections: i64,
    in_use_connections: i64,
    proxy_connections: i64,
}

#[derive(Serialize)]
struct AnalyticsOverviewResponse {
    timestamp: String,
    verbose_pipeline_compiled: bool,
    fleet: FleetTotals,
    by_endpoint_type: Vec<PerTypeRow>,
    by_endpoint: Vec<PerEndpointRow>,
}

/// GET /api/v1/analytics/overview
///
/// Returns live, high-level metrics from the in-process telemetry registry.
/// This endpoint is always available and does not depend on verbose capture
/// or any external store.
///
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
pub async fn overview(
    auth: web::ReqData<ParsedJwt>,
    database_manager: web::Data<EdenDb>,
    all_metrics: web::Data<AllMetrics>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database_manager, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    Ok(HttpResponse::Ok().json(build_overview(all_metrics.get_ref())))
}

fn build_overview(all_metrics: &AllMetrics) -> AnalyticsOverviewResponse {
    let eden = all_metrics.eden();

    // Fleet aggregates — cheap sum_all() reads across all label sets.
    let total_requests = eden.get_request_count();
    let success_count = eden.get_success_count();
    let error_count = eden.get_error_count();
    let error_rate = if total_requests == 0 {
        0.0
    } else {
        error_count as f64 / total_requests as f64
    };

    let fleet = FleetTotals {
        total_requests,
        active_requests: eden.get_active_requests(),
        success_count,
        error_count,
        error_rate,
        upload_bytes: eden.get_upload_bytes(),
        download_bytes: eden.get_download_bytes(),
        open_connections: eden.get_connections(),
        in_use_connections: eden.get_connections_in_use(),
    };

    let by_endpoint_type = aggregate_by_endpoint_type(all_metrics);
    let by_endpoint = aggregate_by_endpoint_uuid();

    AnalyticsOverviewResponse {
        timestamp: chrono::Utc::now().to_rfc3339(),
        verbose_pipeline_compiled: VERBOSE_PIPELINE_COMPILED,
        fleet,
        by_endpoint_type,
        by_endpoint,
    }
}

/// Roll up dynamic-counter snapshots by `endpoint_type`. Series without an
/// `endpoint_type` label (e.g. internal Eden control-plane traffic) fall into
/// the synthetic `"_unspecified"` bucket so they are visible but separable.
fn aggregate_by_endpoint_type(all_metrics: &AllMetrics) -> Vec<PerTypeRow> {
    let eden = all_metrics.eden();
    let mut rows: HashMap<String, PerTypeRow> = HashMap::new();

    for (labels, value) in eden.core.request_count.snapshot() {
        let key = endpoint_type_label(labels.pairs());
        ensure_type_row(&mut rows, &key).request_count = value.max(0) as u64;
    }
    for (labels, value) in eden.core.error_count.snapshot() {
        let key = endpoint_type_label(labels.pairs());
        ensure_type_row(&mut rows, &key).error_count = value.max(0) as u64;
    }
    for (labels, value) in eden.snapshot_active_requests() {
        let key = endpoint_type_label(labels.pairs());
        ensure_type_row(&mut rows, &key).active_requests = value;
    }
    for (labels, value) in eden.snapshot_upload_bytes() {
        let key = endpoint_type_label(labels.pairs());
        ensure_type_row(&mut rows, &key).upload_bytes = value.max(0) as u64;
    }
    for (labels, value) in eden.snapshot_download_bytes() {
        let key = endpoint_type_label(labels.pairs());
        ensure_type_row(&mut rows, &key).download_bytes = value.max(0) as u64;
    }

    // Connection gauges are labelled by `db_type` (which mirrors endpoint kind)
    // and `endpoint_uuid`. Aggregate up to `db_type` here.
    let conn_state = eden_core::telemetry::connection_tracker::connection_state();
    for (db_type, _endpoint_uuid, count) in conn_state.snapshot_endpoint_open() {
        ensure_type_row(&mut rows, db_type).open_connections += count;
    }
    for (db_type, _endpoint_uuid, count) in conn_state.snapshot_endpoint_in_use() {
        ensure_type_row(&mut rows, db_type).in_use_connections += count;
    }

    let mut out: Vec<PerTypeRow> = rows.into_values().collect();
    out.sort_by(|a, b| b.request_count.cmp(&a.request_count).then_with(|| a.endpoint_type.cmp(&b.endpoint_type)));
    out
}

fn ensure_type_row<'a>(rows: &'a mut HashMap<String, PerTypeRow>, key: &str) -> &'a mut PerTypeRow {
    rows.entry(key.to_string()).or_insert_with(|| PerTypeRow {
        endpoint_type: key.to_string(),
        request_count: 0,
        active_requests: 0,
        error_count: 0,
        upload_bytes: 0,
        download_bytes: 0,
        open_connections: 0,
        in_use_connections: 0,
    })
}

fn endpoint_type_label(pairs: &[(String, String)]) -> String {
    for (k, v) in pairs {
        if k == "endpoint_type" {
            return v.clone();
        }
    }
    "_unspecified".to_string()
}

/// Live per-endpoint-instance snapshot from the stable connection registry.
/// This is the low-cost source for live per-endpoint connection state.
fn aggregate_by_endpoint_uuid() -> Vec<PerEndpointRow> {
    let state = eden_core::telemetry::connection_tracker::connection_state();

    let mut rows: HashMap<String, PerEndpointRow> = HashMap::new();

    for (db_type, endpoint_uuid, count) in state.snapshot_endpoint_open() {
        if endpoint_uuid.is_empty() {
            continue;
        }
        let row = rows.entry(endpoint_uuid.clone()).or_insert_with(|| PerEndpointRow {
            endpoint_uuid: endpoint_uuid.clone(),
            endpoint_type: Some(db_type.to_string()),
            open_connections: 0,
            in_use_connections: 0,
            proxy_connections: 0,
        });
        row.open_connections += count;
    }
    for (db_type, endpoint_uuid, count) in state.snapshot_endpoint_in_use() {
        if endpoint_uuid.is_empty() {
            continue;
        }
        let row = rows.entry(endpoint_uuid.clone()).or_insert_with(|| PerEndpointRow {
            endpoint_uuid: endpoint_uuid.clone(),
            endpoint_type: Some(db_type.to_string()),
            open_connections: 0,
            in_use_connections: 0,
            proxy_connections: 0,
        });
        row.in_use_connections += count;
        if row.endpoint_type.is_none() {
            row.endpoint_type = Some(db_type.to_string());
        }
    }
    // Proxy snapshots are keyed by interlay_id (proxy listener) rather than
    // endpoint_uuid, so we surface them as their own rows for visibility.
    for (interlay_id, count) in state.snapshot_proxy() {
        let row = rows.entry(interlay_id.clone()).or_insert_with(|| PerEndpointRow {
            endpoint_uuid: interlay_id,
            endpoint_type: None,
            open_connections: 0,
            in_use_connections: 0,
            proxy_connections: 0,
        });
        row.proxy_connections += count;
    }

    let mut out: Vec<PerEndpointRow> = rows.into_values().collect();
    out.sort_by(|a, b| {
        let a_active = a.in_use_connections + a.proxy_connections;
        let b_active = b.in_use_connections + b.proxy_connections;
        b_active.cmp(&a_active).then_with(|| a.endpoint_uuid.cmp(&b.endpoint_uuid))
    });
    out
}
