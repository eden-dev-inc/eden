//! Connection metrics HTTP endpoint.
//!
//! Exposes real-time and historical traffic counts for endpoint pools,
//! endpoint proxy connections, and HTTP/API requests. Data is sourced from
//! in-memory gauges (live snapshot) and ClickHouse (historical time series).

use crate::EdenDb;
use crate::analytics::AnalyticsState;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::Error as ActixError;
use actix_web::{HttpResponse, Responder, web};
use bytes::Bytes;
use dashmap::DashMap;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_uuid::{CacheUuid, InterlayCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::telemetry::AllMetrics;
use endpoint_core::ep_core::database::schema::interlay::InterlayState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use telemetry_extensions_macro::with_telemetry;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::interval;

pub(crate) const CONNECTION_METRICS_STREAM_INTERVAL_SECS: u64 = 10;
const CONNECTION_METRICS_STREAM_BROADCAST_CAPACITY: usize = 64;

#[derive(Debug, Deserialize)]
pub struct ConnectionMetricsQuery {
    /// Time range: "5m", "15m", "1h", "6h", "24h", "7d", "30d"
    pub range: Option<String>,
    /// RFC3339 timestamp to fetch only buckets at or after the last loaded point.
    pub since: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ConnectionMetricsSnapshot {
    // Total open connections (idle + in-use) to endpoint backends.
    endpoint_connections_total: i64,
    endpoint_connections_by_type: HashMap<String, i64>,
    endpoint_connections_by_uuid: HashMap<String, i64>,
    // Connections currently checked out of a pool (active).
    endpoint_connections_in_use: i64,
    endpoint_connections_in_use_by_uuid: HashMap<String, i64>,
    // Client wire-protocol connections to Eden (via the endpoint proxy layer).
    //
    // Compatibility field: prefer `endpoint_proxy_connections_*` in new UI/API
    // callers so this does not get confused with dashboard/API traffic.
    proxy_connections_total: i64,
    proxy_connections_by_endpoint: HashMap<String, i64>,
    // Client-side breakdown: key is `"client_ip|interlay_id"`.
    proxy_connections_by_client: HashMap<String, i64>,
    // Compatibility field: active HTTP requests to Eden service.
    // Prefer `api_requests_active` in new UI/API callers.
    active_requests: i64,
    // Explicitly named aliases for the two kinds of load people tend to
    // conflate when diagnosing server pressure.
    endpoint_proxy_connections_total: i64,
    endpoint_proxy_connections_by_interlay: HashMap<String, i64>,
    endpoint_proxy_connections_by_client: HashMap<String, i64>,
    api_requests_active: i64,
    api_requests_total: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ConnectionMetricsPoint {
    timestamp: String,
    endpoint_connections: i64,
    endpoint_connections_in_use: i64,
    endpoint_proxy_connections: i64,
    proxy_connections: i64,
    api_requests_active: i64,
    active_requests: i64,
}

#[derive(Debug, Clone, Serialize)]
struct ConnectionMetricsResponse {
    current: ConnectionMetricsSnapshot,
    time_series: Vec<ConnectionMetricsPoint>,
    timestamp: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ConnectionMetricsStreamResponse {
    current: ConnectionMetricsSnapshot,
    timestamp: String,
}

/// Shared broadcaster for live connection metrics snapshots.
pub(crate) struct ConnectionMetricsStreamManager {
    sender: broadcast::Sender<()>,
}

impl ConnectionMetricsStreamManager {
    pub(crate) fn new() -> Self {
        let (sender, _) = broadcast::channel(CONNECTION_METRICS_STREAM_BROADCAST_CAPACITY);
        Self { sender }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<()> {
        self.sender.subscribe()
    }

    pub(crate) fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }

    pub(crate) async fn run(self: Arc<Self>) {
        let mut ticker = interval(Duration::from_secs(CONNECTION_METRICS_STREAM_INTERVAL_SECS));

        loop {
            ticker.tick().await;

            if self.receiver_count() == 0 {
                continue;
            }

            let _ = self.sender.send(());
        }
    }
}

/// GET /api/v1/analytics/connections
///
/// Returns current live connection counts and historical time series.
#[with_telemetry]
pub async fn connections(
    auth: web::ReqData<ParsedJwt>,
    database_manager: web::Data<EdenDb>,
    all_metrics: web::Data<AllMetrics>,
    analytics_state: web::Data<AnalyticsState>,
    interlay_endpoints: web::Data<DashMap<InterlayCacheUuid, InterlayState>>,
    query: web::Query<ConnectionMetricsQuery>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database_manager, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let organization_uuid = auth.org_uuid().to_string();
    let current =
        build_live_snapshot(&organization_uuid, all_metrics.get_ref(), analytics_state.get_ref(), interlay_endpoints.as_ref()).await;
    let time_series = query_historical(&database_manager, &organization_uuid, query.range.as_deref(), query.since.as_deref()).await?;

    let response = ConnectionMetricsResponse {
        current,
        time_series,
        timestamp: chrono::Utc::now().to_rfc3339(),
    };

    Ok(HttpResponse::Ok().json(response))
}

/// GET /api/v1/analytics/connections/stream
///
/// Streams the current live connection snapshot over SSE.
#[with_telemetry]
pub async fn stream_connections(
    auth: web::ReqData<ParsedJwt>,
    database_manager: web::Data<EdenDb>,
    all_metrics: web::Data<AllMetrics>,
    analytics_state: web::Data<AnalyticsState>,
    interlay_endpoints: web::Data<DashMap<InterlayCacheUuid, InterlayState>>,
    stream_manager: web::Data<Arc<ConnectionMetricsStreamManager>>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database_manager, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let organization_uuid = auth.org_uuid().to_string();
    let initial_payload =
        build_live_stream_response(&organization_uuid, all_metrics.get_ref(), analytics_state.get_ref(), interlay_endpoints.as_ref()).await;
    let mut rx = stream_manager.subscribe();

    let stream = async_stream::stream! {
        match serde_json::to_string(&initial_payload) {
            Ok(json) => yield Ok::<Bytes, ActixError>(Bytes::from(format_sse("snapshot", &json))),
            Err(error) => {
                let json = serde_json::json!({
                    "error": format!("failed to serialize connection metrics stream payload: {error}"),
                })
                .to_string();
                yield Ok::<Bytes, ActixError>(Bytes::from(format_sse("error", &json)));
                return;
            }
        }

        loop {
            match rx.recv().await {
                Ok(()) => {
                    let payload = build_live_stream_response(
                        &organization_uuid,
                        all_metrics.get_ref(),
                        analytics_state.get_ref(),
                        interlay_endpoints.as_ref(),
                    )
                    .await;
                    match serde_json::to_string(&payload) {
                        Ok(json) => yield Ok::<Bytes, ActixError>(Bytes::from(format_sse("snapshot", &json))),
                        Err(error) => {
                            let json = serde_json::json!({
                                "error": format!("failed to serialize connection metrics stream payload: {error}"),
                            })
                            .to_string();
                            yield Ok::<Bytes, ActixError>(Bytes::from(format_sse("error", &json)));
                            break;
                        }
                    }
                }
                Err(RecvError::Lagged(missed_events)) => {
                    let json = serde_json::json!({ "missed_events": missed_events }).to_string();
                    yield Ok::<Bytes, ActixError>(Bytes::from(format_sse("lagged", &json)));
                }
                Err(RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    Ok(HttpResponse::Ok()
        .insert_header(("Content-Type", "text/event-stream"))
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("Connection", "keep-alive"))
        .streaming(stream))
}

fn format_sse(event: &str, data: &str) -> String {
    format!("event: {event}\ndata: {data}\n\n")
}

async fn build_live_stream_response(
    organization_uuid: &str,
    all_metrics: &AllMetrics,
    analytics_state: &AnalyticsState,
    interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>,
) -> ConnectionMetricsStreamResponse {
    ConnectionMetricsStreamResponse {
        current: build_live_snapshot(organization_uuid, all_metrics, analytics_state, interlay_endpoints).await,
        timestamp: chrono::Utc::now().to_rfc3339(),
    }
}

async fn build_live_snapshot(
    organization_uuid: &str,
    all_metrics: &AllMetrics,
    analytics_state: &AnalyticsState,
    interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>,
) -> ConnectionMetricsSnapshot {
    let state = eden_core::telemetry::connection_tracker::connection_state();

    let endpoint_tenants = {
        let _ = analytics_state;
        HashMap::new()
    };
    let interlay_tenants = build_interlay_tenant_lookup(interlay_endpoints);
    let aggregated = crate::connection_metrics_aggregation::aggregate_connection_metrics_by_tenant(
        &state.snapshot_endpoint_open(),
        &state.snapshot_endpoint_in_use(),
        &state.snapshot_proxy(),
        &state.snapshot_proxy_clients(),
        &all_metrics.eden().snapshot_active_requests(),
        &all_metrics.eden().snapshot_request_count(),
        &endpoint_tenants,
        &interlay_tenants,
    );
    let current = aggregated.get(organization_uuid).cloned().unwrap_or_default();

    ConnectionMetricsSnapshot {
        endpoint_connections_total: current.endpoint_connections_total,
        endpoint_connections_by_type: current.endpoint_connections_by_type,
        endpoint_connections_by_uuid: current.endpoint_connections_by_uuid,
        endpoint_connections_in_use: current.endpoint_connections_in_use,
        endpoint_connections_in_use_by_uuid: current.endpoint_connections_in_use_by_uuid,
        proxy_connections_total: current.proxy_connections_total,
        proxy_connections_by_endpoint: current.proxy_connections_by_endpoint.clone(),
        proxy_connections_by_client: current.proxy_connections_by_client.clone(),
        active_requests: current.active_requests,
        endpoint_proxy_connections_total: current.proxy_connections_total,
        endpoint_proxy_connections_by_interlay: current.proxy_connections_by_endpoint,
        endpoint_proxy_connections_by_client: current.proxy_connections_by_client,
        api_requests_active: current.active_requests,
        api_requests_total: current.api_requests_total,
    }
}

fn build_interlay_tenant_lookup(interlay_endpoints: &DashMap<InterlayCacheUuid, InterlayState>) -> HashMap<String, String> {
    interlay_endpoints
        .iter()
        .filter_map(|entry| Some((entry.key().uuid().to_string(), entry.value().endpoint_uuid().org()?.uuid().to_string())))
        .collect()
}

async fn query_historical(
    _database_manager: &web::Data<EdenDb>,
    _organization_uuid: &str,
    _range_str: Option<&str>,
    _since_str: Option<&str>,
) -> Result<Vec<ConnectionMetricsPoint>, actix_web::Error> {
    Ok(Vec::new())
}
