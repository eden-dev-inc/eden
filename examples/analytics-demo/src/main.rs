// Analytics Server - Customer-style e-commerce backend with analytics attached.
//
// Supports Redis-only, PostgreSQL-only, or dual-backend operation from one build.

use anyhow::{Error, Result as AnyResult};
use axum::extract::MatchedPath;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use clap::Parser;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::sync::atomic::AtomicU64;
use std::{future::Future, net::IpAddr, sync::Arc, time::Duration};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

mod activity;
mod config;
mod datadog;
mod generators;
mod metrics;
mod models;
mod redis;
mod runtime_controls;
mod telemetry;
mod workers;

mod postgres;

use config::Config;
use generators::DataGenerator;
use metrics::AppMetrics;
use models::{
    AnalyticsOverview, CartSnapshot, CatalogProduct, CatalogResponse, Event, EventType,
    EventTypeDistribution, HourlyMetrics, Organization, StorefrontInventoryAlert,
    StorefrontOrderSummary, StorefrontProductRevenue, StorefrontResponse, TopPage,
};
use postgres::workers::{PgEventWriterWorker, PgQuerySimulatorWorker};
use postgres::Database;
use redis::workers::SyntheticDataGenerator;
use redis::workers::{CacheWarmupWorker, EventSimulatorWorker, QuerySimulatorWorker};
use redis::RedisCache;
use runtime_controls::{RuntimeControlPatch, RuntimeControlSettings, RuntimeControls};
use telemetry::{
    init_tracing, install_legacy_telemetry_env_aliases, wait_for_shutdown_signal, ActivityEmission,
    TelemetryRuntime, TelemetrySpanKind,
};
use workers::{OrgIdCache, SystemMonitorWorker};

const OVERVIEW_TTL_SECONDS: u64 = 300;
const HOURLY_TTL_SECONDS: u64 = 300;
const TOP_PAGES_TTL_SECONDS: u64 = 600;
const EVENT_DISTRIBUTION_TTL_SECONDS: u64 = 300;
const STOREFRONT_TTL_SECONDS: u64 = 120;
const CATALOG_TTL_SECONDS: u64 = 300;
const CART_TTL_SECONDS: u64 = 120;
const TOP_PAGES_CACHE_LIMIT: usize = 50;
const CATALOG_CACHE_LIMIT: usize = 50;

#[derive(Clone)]
struct AppState {
    cache: Option<Arc<RedisCache>>,
    controls: Arc<RuntimeControls>,
    telemetry: Arc<TelemetryRuntime>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
    config: Arc<Config>,
    db: Option<Arc<Database>>,
    shutdown: CancellationToken,
}

#[derive(Serialize)]
struct HealthStatus {
    status: &'static str,
    mode: &'static str,
    internal_workload_enabled: bool,
    redis_enabled: bool,
    redis_connected: bool,
    postgres_enabled: bool,
    postgres_connected: bool,
}

#[derive(Debug, Deserialize)]
struct OrganizationsQuery {
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct OverviewQuery {
    hours: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct TopPagesQuery {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct HourlyMetricsQuery {
    points: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct DashboardQuery {
    hours: Option<u32>,
    hourly_points: Option<u32>,
    top_pages_limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct CatalogQuery {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct TrackEventRequest {
    user_id: Option<Uuid>,
    event_type: String,
    page_url: Option<String>,
    referrer: Option<String>,
    user_agent: Option<String>,
    ip_address: Option<String>,
    properties: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct CreateCartRequest {
    user_id: Option<Uuid>,
    product_id: Option<Uuid>,
    quantity: Option<i32>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct AddCartItemRequest {
    product_id: Option<Uuid>,
    quantity: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct CheckoutCartRequest {
    user_id: Option<Uuid>,
    payment_method: Option<String>,
    notes: Option<String>,
}

#[derive(Debug, Serialize)]
struct DashboardResponse {
    organization_id: Uuid,
    generated_at: chrono::DateTime<Utc>,
    overview: AnalyticsOverview,
    hourly_metrics: Vec<HourlyMetrics>,
    top_pages: Vec<TopPage>,
    event_distribution: EventTypeDistribution,
}

#[derive(Debug, Serialize)]
struct TrackEventResponse {
    accepted: bool,
    event_id: Uuid,
    organization_id: Uuid,
    persisted_to_postgres: bool,
    invalidated_cache_keys: usize,
    created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct CartMutationResponse {
    accepted: bool,
    organization_id: Uuid,
    cart_id: Uuid,
    status: String,
    item_count: i32,
    subtotal_cents: i64,
    total_cents: i64,
    invalidated_cache_keys: usize,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct CheckoutCartResponse {
    accepted: bool,
    organization_id: Uuid,
    cart_id: Uuid,
    order_id: Uuid,
    payment_id: Uuid,
    total_cents: i64,
    currency: String,
    invalidated_cache_keys: usize,
    created_at: chrono::DateTime<Utc>,
}

fn build_app(state: AppState) -> Router {
    let router = Router::new()
        .route("/api/v1/organizations", get(list_organizations_handler))
        .route(
            "/api/v1/organizations/:org_id/dashboard",
            get(dashboard_handler),
        )
        .route(
            "/api/v1/organizations/:org_id/analytics/overview",
            get(overview_handler),
        )
        .route(
            "/api/v1/organizations/:org_id/analytics/top-pages",
            get(top_pages_handler),
        )
        .route(
            "/api/v1/organizations/:org_id/analytics/hourly",
            get(hourly_metrics_handler),
        )
        .route(
            "/api/v1/organizations/:org_id/storefront",
            get(storefront_handler),
        )
        .route(
            "/api/v1/organizations/:org_id/catalog",
            get(catalog_handler),
        )
        .route(
            "/api/v1/organizations/:org_id/carts",
            post(create_cart_handler),
        )
        .route(
            "/api/v1/organizations/:org_id/carts/:cart_id",
            get(cart_detail_handler),
        )
        .route(
            "/api/v1/organizations/:org_id/carts/:cart_id/items",
            post(add_cart_item_handler),
        )
        .route(
            "/api/v1/organizations/:org_id/carts/:cart_id/checkout",
            post(checkout_cart_handler),
        )
        .route(
            "/api/v1/organizations/:org_id/events",
            post(track_event_handler),
        )
        .route("/health", get(health_handler));
    let router = if state.config.internal_workload_enabled {
        router.route(
            "/control",
            get(control_handler).patch(update_control_handler),
        )
    } else {
        router
    };
    router
        .layer(middleware::from_fn_with_state(
            state.clone(),
            server_http_telemetry_middleware,
        ))
        .with_state(state)
}

pub async fn build_test_app(config: Config) -> Router {
    let generator = Arc::new(DataGenerator::new());
    let org_cache = Arc::new(OrgIdCache::new());
    org_cache
        .initialize_synthetic(config.organizations, config.users_per_org)
        .await;

    let controls = RuntimeControls::from_config(&config);
    let shutdown = CancellationToken::new();
    let telemetry =
        TelemetryRuntime::from_options(config.telemetry_options(), config.backend_mode_label());

    build_app(AppState {
        cache: None,
        controls,
        telemetry,
        generator,
        org_cache,
        config: Arc::new(config),
        db: None,
        shutdown,
    })
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    init_tracing("analytics_server=info,sqlx=warn");
    install_legacy_telemetry_env_aliases();

    let config = Config::parse();
    config.validate().map_err(Error::msg)?;
    let mode_label = config.backend_mode_label();

    let cache = if config.redis_enabled {
        info!("Initializing Redis connection pool...");
        match RedisCache::new(&config.redis_url, config.redis_pool_size).await {
            Ok(c) => {
                info!("Redis connected successfully");
                Some(Arc::new(c))
            }
            Err(e) => {
                return Err(Error::msg(format!(
                    "failed to initialize Redis backend at {}: {}",
                    config.redis_url, e
                )));
            }
        }
    } else {
        info!("Redis backend disabled by configuration");
        None
    };

    info!("Starting analytics-server ({})", mode_label);
    info!("Configuration:");
    info!("  - Target QPS: {}", config.queries_per_second);
    info!("  - Events/sec: {}", config.events_per_second);
    info!(
        "  - Internal workload enabled: {}",
        config.internal_workload_enabled
    );
    info!("  - Organizations: {}", config.organizations);
    info!("  - Max workers: {}", config.max_workers);
    info!("  - Redis enabled: {}", config.redis_enabled);
    if config.redis_enabled {
        info!("  - Redis pool size: {}", config.redis_pool_size);
        info!("  - Redis URL: {}", config.redis_url);
    }
    info!("  - PostgreSQL enabled: {}", config.postgres_enabled);
    if config.postgres_enabled {
        info!(
            "  - PostgreSQL: {}:{}/{}",
            config.postgres_host, config.postgres_port, config.postgres_database
        );
        info!("  - PostgreSQL pool: {}", config.db_pool_size);
        info!("  - PG query workers: {}", config.pg_query_workers);
        info!("  - PG events/sec: {}", config.pg_events_per_second);
    }
    info!("  - Backends: {}", mode_label);

    let generator = Arc::new(DataGenerator::new());
    let org_cache = Arc::new(OrgIdCache::new());
    let controls = RuntimeControls::from_config(&config);
    let shutdown = CancellationToken::new();
    controls.start_background_tasks(shutdown.clone());
    let telemetry = TelemetryRuntime::from_options(config.telemetry_options(), mode_label);
    if telemetry.enabled() {
        telemetry.emit_startup(&config);
        telemetry.emit_runtime_control_update(&controls.snapshot());
    }

    let db = if config.postgres_enabled {
        info!("Initializing PostgreSQL connection pool...");
        let pg_url = config.postgres_url();
        info!(
            "Connecting to PostgreSQL at {}:{}/{}",
            config.postgres_host, config.postgres_port, config.postgres_database
        );
        let database = Arc::new(Database::new(&pg_url, config.db_pool_size).await?);
        info!("Setting up PostgreSQL schema...");
        database.setup_schema().await?;
        info!("Seeding initial data...");
        database.seed_initial_data(&generator, &config).await?;
        Some(database)
    } else {
        info!("PostgreSQL backend disabled by configuration");
        None
    };

    // Initialize org/user data from PostgreSQL when available, otherwise synthetic.
    if let Some(db) = db.as_ref() {
        info!("Initializing organization data from PostgreSQL...");
        org_cache
            .initialize_from_db(db, config.organizations, config.users_per_org)
            .await?;
    } else {
        info!("Initializing synthetic organization data...");
        org_cache
            .initialize_synthetic(config.organizations, config.users_per_org)
            .await;
    }

    let state = AppState {
        cache: cache.clone(),
        controls: controls.clone(),
        telemetry: telemetry.clone(),
        generator: generator.clone(),
        org_cache: org_cache.clone(),
        config: Arc::new(config.clone()),
        db: db.clone(),
        shutdown: shutdown.clone(),
    };

    // Start Redis workers only if Redis is available
    let redis_keys_written = Arc::new(AtomicU64::new(0));
    if state.cache.is_some() {
        tokio::spawn(start_cache_warmup(state.clone()));
        if state.config.internal_workload_enabled {
            tokio::spawn(start_event_simulator(state.clone(), redis_keys_written));
            tokio::spawn(start_query_simulator(state.clone()));
        } else {
            info!("Skipping Redis traffic simulators (internal workload disabled)");
        }
    } else {
        info!("Skipping Redis workers (no Redis connection)");
    }

    // Start system monitor
    tokio::spawn(start_system_monitor(state.clone()));

    if state.db.is_some() {
        if state.config.internal_workload_enabled {
            tokio::spawn(start_pg_query_simulator(state.clone()));
            tokio::spawn(start_pg_event_writer(state.clone()));
        } else {
            info!("Skipping PostgreSQL traffic simulators (internal workload disabled)");
        }
    } else {
        info!("Skipping PostgreSQL workers (backend disabled)");
    }

    let app = build_app(state.clone());

    let listener = tokio::net::TcpListener::bind(&config.bind_address).await?;

    info!("===========================================");
    info!("Analytics Server Ready ({})", mode_label);
    info!("===========================================");
    info!("Health endpoint: http://{}/health", config.bind_address);
    if config.internal_workload_enabled {
        info!(
            "Legacy control endpoint: http://{}/control",
            config.bind_address
        );
    }
    info!("Target throughput: {} QPS", config.queries_per_second);
    info!("Simulated events: {}/sec", config.events_per_second);
    info!("Organizations: {}", config.organizations);
    if config.postgres_enabled {
        info!("PG query workers: {}", config.pg_query_workers);
        info!("PG write events/sec: {}", config.pg_events_per_second);
    }
    info!("===========================================");

    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown_signal())
        .await?;
    state.shutdown.cancel();
    state.telemetry.shutdown().await;
    Ok(())
}

async fn health_handler(State(state): State<AppState>) -> Json<HealthStatus> {
    Json(HealthStatus {
        status: "ok",
        mode: state.config.backend_mode_label(),
        internal_workload_enabled: state.config.internal_workload_enabled,
        redis_enabled: state.config.redis_enabled,
        redis_connected: state.cache.is_some(),
        postgres_enabled: state.config.postgres_enabled,
        postgres_connected: state.db.is_some(),
    })
}

async fn server_http_telemetry_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let traceparent = request
        .headers()
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let matched_path = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or_else(|| request.uri().path())
        .to_string();
    let method = request.method().as_str().to_string();
    let mut span = state.telemetry.tracer().start_span_from_traceparent(
        traceparent.as_deref(),
        "analytics_server.http",
        TelemetrySpanKind::Server,
    );
    span.enter();
    span.set_attribute("http.route", &matched_path);
    span.set_attribute("http.method", &method);
    let start = std::time::Instant::now();

    let response = next.run(request).await;
    span.set_attribute("http.status_code", response.status().as_u16());
    if response.status().is_server_error() {
        span.record_error("http_error");
    }
    state.telemetry.metrics().record_http_request(
        "server",
        &matched_path,
        &method,
        response.status().as_u16(),
        start.elapsed().as_secs_f64(),
    );
    span.finish();
    response
}

async fn control_handler(State(state): State<AppState>) -> Json<RuntimeControlSettings> {
    Json(state.controls.snapshot())
}

async fn update_control_handler(
    State(state): State<AppState>,
    Json(patch): Json<RuntimeControlPatch>,
) -> std::result::Result<Json<RuntimeControlSettings>, (StatusCode, String)> {
    match state.controls.apply_patch(patch) {
        Ok(settings) => {
            info!(
                "Updated runtime controls: queries_per_second={} events_per_second={}",
                settings.queries_per_second, settings.events_per_second
            );
            state.telemetry.emit_runtime_control_update(&settings);
            Ok(Json(settings))
        }
        Err(message) => Err((StatusCode::BAD_REQUEST, message)),
    }
}

async fn list_organizations_handler(
    State(state): State<AppState>,
    Query(query): Query<OrganizationsQuery>,
) -> std::result::Result<Json<Vec<Organization>>, (StatusCode, String)> {
    let limit = query.limit.unwrap_or(20).clamp(1, 200);
    let start = std::time::Instant::now();

    if let Some(db) = state.db.as_ref() {
        let mut span = state.telemetry.tracer().start_span(
            "analytics_server.postgres.list_organizations",
            TelemetrySpanKind::Client,
        );
        span.enter();
        span.set_attribute("db.system", "postgresql");
        span.set_attribute("db.operation", "list_organizations");
        span.set_attribute("analytics.limit", limit as i64);
        let result = db.list_organizations(limit).await;
        match result {
            Ok(organizations) => {
                span.set_attribute("db.result_count", organizations.len() as i64);
                span.finish();
                let latency_ns = start.elapsed().as_nanos() as u64;
                state.telemetry.metrics().record_live_latency_ns(latency_ns);
                state
                    .telemetry
                    .metrics()
                    .record_operation_success("customer_organizations_list");
                state.telemetry.emit_custom_activity(ActivityEmission {
                    descriptor: activity::query("organization_list"),
                    org_id: None,
                    status: "success",
                    latency_us: Some(latency_ns as f64 / 1000.0),
                    error_type: None,
                    extra_tags: vec![
                        "role:server".to_string(),
                        format!("result_count:{}", organizations.len()),
                    ],
                    payload: &json!({
                        "limit": limit,
                        "result_count": organizations.len(),
                        "source": "postgres",
                    }),
                });
                Ok(Json(organizations))
            }
            Err(error) => {
                span.record_error("query_execution_error");
                span.set_attribute("error.message", error.to_string());
                span.finish();
                let latency_ns = start.elapsed().as_nanos() as u64;
                state
                    .telemetry
                    .metrics()
                    .record_operation_error("customer_organizations_list", "query_execution_error");
                state.telemetry.emit_custom_activity(ActivityEmission {
                    descriptor: activity::query("organization_list"),
                    org_id: None,
                    status: "error",
                    latency_us: Some(latency_ns as f64 / 1000.0),
                    error_type: Some("query_execution_error"),
                    extra_tags: vec!["role:server".to_string()],
                    payload: &json!({
                        "limit": limit,
                        "error_message": error.to_string(),
                    }),
                });
                Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("failed to list organizations: {}", error),
                ))
            }
        }
    } else {
        let organizations: Vec<Organization> = state
            .org_cache
            .get_org_ids()
            .await
            .into_iter()
            .take(limit as usize)
            .enumerate()
            .map(|(index, id)| Organization {
                id,
                name: format!("Demo Organization {}", index + 1),
                created_at: Utc::now() - chrono::Duration::days(index as i64),
            })
            .collect();

        state
            .telemetry
            .metrics()
            .record_operation_success("customer_organizations_list");
        let latency_ns = start.elapsed().as_nanos() as u64;
        state.telemetry.metrics().record_live_latency_ns(latency_ns);
        state.telemetry.emit_custom_activity(ActivityEmission {
            descriptor: activity::query("organization_list"),
            org_id: None,
            status: "success",
            latency_us: Some(latency_ns as f64 / 1000.0),
            error_type: None,
            extra_tags: vec![
                "role:server".to_string(),
                format!("result_count:{}", organizations.len()),
            ],
            payload: &json!({
                "limit": limit,
                "result_count": organizations.len(),
                "source": "synthetic",
            }),
        });
        Ok(Json(organizations))
    }
}

async fn dashboard_handler(
    State(state): State<AppState>,
    Path(org_id): Path<Uuid>,
    Query(query): Query<DashboardQuery>,
) -> std::result::Result<Json<DashboardResponse>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;
    let start = std::time::Instant::now();

    let hours = query.hours.unwrap_or(24).clamp(1, 168);
    let hourly_points = query.hourly_points.unwrap_or(6).clamp(1, 24);
    let top_pages_limit = query
        .top_pages_limit
        .unwrap_or(10)
        .clamp(1, TOP_PAGES_CACHE_LIMIT);

    let overview = load_overview(&state, org_id, hours).await?;
    let hourly_metrics = load_hourly_series(&state, org_id, hourly_points).await?;
    let top_pages = load_top_pages(&state, org_id, top_pages_limit).await?;
    let event_distribution = load_event_distribution(&state, org_id).await?;

    state
        .telemetry
        .metrics()
        .record_operation_success("customer_dashboard_load");
    let latency_ns = start.elapsed().as_nanos() as u64;
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::query("dashboard"),
        org_id: Some(org_id),
        status: "success",
        latency_us: Some(latency_ns as f64 / 1000.0),
        error_type: None,
        extra_tags: vec!["role:server".to_string()],
        payload: &json!({
            "hours": hours,
            "hourly_points": hourly_points,
            "top_pages_limit": top_pages_limit,
        }),
    });

    Ok(Json(DashboardResponse {
        organization_id: org_id,
        generated_at: Utc::now(),
        overview,
        hourly_metrics,
        top_pages,
        event_distribution,
    }))
}

async fn overview_handler(
    State(state): State<AppState>,
    Path(org_id): Path<Uuid>,
    Query(query): Query<OverviewQuery>,
) -> std::result::Result<Json<AnalyticsOverview>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;
    let hours = query.hours.unwrap_or(24).clamp(1, 168);

    load_overview(&state, org_id, hours).await.map(Json)
}

async fn top_pages_handler(
    State(state): State<AppState>,
    Path(org_id): Path<Uuid>,
    Query(query): Query<TopPagesQuery>,
) -> std::result::Result<Json<Vec<TopPage>>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;
    let limit = query.limit.unwrap_or(10).clamp(1, TOP_PAGES_CACHE_LIMIT);

    load_top_pages(&state, org_id, limit).await.map(Json)
}

async fn hourly_metrics_handler(
    State(state): State<AppState>,
    Path(org_id): Path<Uuid>,
    Query(query): Query<HourlyMetricsQuery>,
) -> std::result::Result<Json<Vec<HourlyMetrics>>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;
    let points = query.points.unwrap_or(6).clamp(1, 24);

    load_hourly_series(&state, org_id, points).await.map(Json)
}

async fn storefront_handler(
    State(state): State<AppState>,
    Path(org_id): Path<Uuid>,
) -> std::result::Result<Json<StorefrontResponse>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;
    load_storefront(&state, org_id).await.map(Json)
}

async fn catalog_handler(
    State(state): State<AppState>,
    Path(org_id): Path<Uuid>,
    Query(query): Query<CatalogQuery>,
) -> std::result::Result<Json<CatalogResponse>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;
    let limit = query.limit.unwrap_or(12).clamp(1, CATALOG_CACHE_LIMIT);
    load_catalog(&state, org_id, limit).await.map(Json)
}

async fn cart_detail_handler(
    State(state): State<AppState>,
    Path((org_id, cart_id)): Path<(Uuid, Uuid)>,
) -> std::result::Result<Json<CartSnapshot>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;
    load_cart_snapshot(&state, org_id, cart_id).await.map(Json)
}

async fn create_cart_handler(
    State(state): State<AppState>,
    Path(org_id): Path<Uuid>,
    Json(request): Json<CreateCartRequest>,
) -> std::result::Result<Json<CartMutationResponse>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "cart creation requires POSTGRES_ENABLED=true".to_string(),
        )
    })?;

    let quantity = request.quantity.unwrap_or(1).clamp(1, 25);
    let metadata = request
        .metadata
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let start = std::time::Instant::now();
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.postgres.create_cart",
        TelemetrySpanKind::Client,
    );
    span.enter();
    span.set_attribute("db.system", "postgresql");
    span.set_attribute("db.operation", "create_cart");
    span.set_attribute("organization.id", org_id.to_string());
    span.set_attribute("commerce.quantity", quantity);

    let cart = match db
        .create_cart_with_item(
            org_id,
            request.user_id,
            request.product_id,
            quantity,
            &metadata,
        )
        .await
    {
        Ok(cart) => {
            span.set_attribute("cart.id", cart.id.to_string());
            span.finish();
            cart
        }
        Err(error) => {
            span.record_error("query_execution_error");
            span.set_attribute("error.message", error.to_string());
            span.finish();
            state
                .telemetry
                .metrics()
                .record_operation_error("customer_cart_create", "query_execution_error");
            let latency_ns = start.elapsed().as_nanos() as u64;
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::cart_created(),
                org_id: Some(org_id),
                status: "error",
                latency_us: Some(latency_ns as f64 / 1000.0),
                error_type: Some("query_execution_error"),
                extra_tags: vec!["role:server".to_string()],
                payload: &json!({
                    "error_message": error.to_string(),
                    "quantity": quantity,
                }),
            });
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to create cart: {}", error),
            ));
        }
    };

    if let Err(error) = cache_cart_snapshot(&state, &cart).await {
        error!("failed to cache cart snapshot after create: {}", error);
    }

    let invalidated_cache_keys = match invalidate_analytics_cache(
        &state,
        org_id,
        cart.user_id,
        "click",
        Some("https://app.example.com/storefront"),
        false,
        cart.updated_at,
    )
    .await
    {
        Ok(count) => count,
        Err(error) => {
            error!(
                "failed to invalidate analytics cache after cart create: {}",
                error
            );
            0
        }
    };

    let latency_ns = start.elapsed().as_nanos() as u64;
    state
        .telemetry
        .metrics()
        .record_operation_success("customer_cart_create");
    state.telemetry.metrics().record_live_latency_ns(latency_ns);
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::cart_created(),
        org_id: Some(org_id),
        status: "success",
        latency_us: Some(latency_ns as f64 / 1000.0),
        error_type: None,
        extra_tags: vec![
            "role:server".to_string(),
            format!("cart_id:{}", cart.id),
            format!("item_count:{}", cart.item_count),
        ],
        payload: &json!({
            "cart_id": cart.id,
            "item_count": cart.item_count,
            "subtotal_cents": cart.subtotal_cents,
            "total_cents": cart.total_cents,
            "quantity": quantity,
        }),
    });

    Ok(Json(CartMutationResponse {
        accepted: true,
        organization_id: org_id,
        cart_id: cart.id,
        status: cart.status,
        item_count: cart.item_count,
        subtotal_cents: cart.subtotal_cents,
        total_cents: cart.total_cents,
        invalidated_cache_keys,
        updated_at: cart.updated_at,
    }))
}

async fn add_cart_item_handler(
    State(state): State<AppState>,
    Path((org_id, cart_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<AddCartItemRequest>,
) -> std::result::Result<Json<CartMutationResponse>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "cart updates require POSTGRES_ENABLED=true".to_string(),
        )
    })?;

    let quantity = request.quantity.unwrap_or(1).clamp(1, 25);
    let start = std::time::Instant::now();
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.postgres.add_cart_item",
        TelemetrySpanKind::Client,
    );
    span.enter();
    span.set_attribute("db.system", "postgresql");
    span.set_attribute("db.operation", "add_cart_item");
    span.set_attribute("organization.id", org_id.to_string());
    span.set_attribute("cart.id", cart_id.to_string());
    span.set_attribute("commerce.quantity", quantity);

    let cart = match db
        .add_item_to_cart(org_id, cart_id, request.product_id, quantity)
        .await
    {
        Ok(Some(cart)) => {
            span.finish();
            cart
        }
        Ok(None) => {
            span.record_error("validation_error");
            span.finish();
            return Err((
                StatusCode::NOT_FOUND,
                format!("cart {} was not found or is no longer active", cart_id),
            ));
        }
        Err(error) => {
            span.record_error("query_execution_error");
            span.set_attribute("error.message", error.to_string());
            span.finish();
            state
                .telemetry
                .metrics()
                .record_operation_error("customer_cart_add_item", "query_execution_error");
            let latency_ns = start.elapsed().as_nanos() as u64;
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::cart_item_added(),
                org_id: Some(org_id),
                status: "error",
                latency_us: Some(latency_ns as f64 / 1000.0),
                error_type: Some("query_execution_error"),
                extra_tags: vec!["role:server".to_string(), format!("cart_id:{}", cart_id)],
                payload: &json!({
                    "error_message": error.to_string(),
                    "quantity": quantity,
                }),
            });
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to add cart item: {}", error),
            ));
        }
    };

    if let Err(error) = cache_cart_snapshot(&state, &cart).await {
        error!("failed to cache cart snapshot after add-item: {}", error);
    }

    let invalidated_cache_keys = match invalidate_analytics_cache(
        &state,
        org_id,
        cart.user_id,
        "click",
        Some("https://app.example.com/cart"),
        false,
        cart.updated_at,
    )
    .await
    {
        Ok(count) => count,
        Err(error) => {
            error!(
                "failed to invalidate analytics cache after cart add-item: {}",
                error
            );
            0
        }
    };

    let latency_ns = start.elapsed().as_nanos() as u64;
    state
        .telemetry
        .metrics()
        .record_operation_success("customer_cart_add_item");
    state.telemetry.metrics().record_live_latency_ns(latency_ns);
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::cart_item_added(),
        org_id: Some(org_id),
        status: "success",
        latency_us: Some(latency_ns as f64 / 1000.0),
        error_type: None,
        extra_tags: vec![
            "role:server".to_string(),
            format!("cart_id:{}", cart.id),
            format!("item_count:{}", cart.item_count),
        ],
        payload: &json!({
            "cart_id": cart.id,
            "item_count": cart.item_count,
            "subtotal_cents": cart.subtotal_cents,
            "total_cents": cart.total_cents,
            "quantity": quantity,
        }),
    });

    Ok(Json(CartMutationResponse {
        accepted: true,
        organization_id: org_id,
        cart_id: cart.id,
        status: cart.status,
        item_count: cart.item_count,
        subtotal_cents: cart.subtotal_cents,
        total_cents: cart.total_cents,
        invalidated_cache_keys,
        updated_at: cart.updated_at,
    }))
}

async fn checkout_cart_handler(
    State(state): State<AppState>,
    Path((org_id, cart_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<CheckoutCartRequest>,
) -> std::result::Result<Json<CheckoutCartResponse>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "checkout requires POSTGRES_ENABLED=true".to_string(),
        )
    })?;

    let payment_method = sanitize_payment_method(request.payment_method.as_deref());
    let start = std::time::Instant::now();
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.postgres.checkout_cart",
        TelemetrySpanKind::Client,
    );
    span.enter();
    span.set_attribute("db.system", "postgresql");
    span.set_attribute("db.operation", "checkout_cart");
    span.set_attribute("organization.id", org_id.to_string());
    span.set_attribute("cart.id", cart_id.to_string());
    span.set_attribute("commerce.payment_method", payment_method);

    let receipt = match db
        .checkout_cart(
            org_id,
            cart_id,
            request.user_id,
            payment_method,
            request.notes.as_deref(),
        )
        .await
    {
        Ok(Some(receipt)) => {
            span.set_attribute("order.id", receipt.order_id.to_string());
            span.finish();
            receipt
        }
        Ok(None) => {
            span.record_error("validation_error");
            span.finish();
            return Err((
                StatusCode::NOT_FOUND,
                format!("cart {} was not found or is no longer active", cart_id),
            ));
        }
        Err(error) => {
            span.record_error("query_execution_error");
            span.set_attribute("error.message", error.to_string());
            span.finish();
            state
                .telemetry
                .metrics()
                .record_operation_error("customer_checkout", "query_execution_error");
            let latency_ns = start.elapsed().as_nanos() as u64;
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::cart_checked_out(),
                org_id: Some(org_id),
                status: "error",
                latency_us: Some(latency_ns as f64 / 1000.0),
                error_type: Some("query_execution_error"),
                extra_tags: vec!["role:server".to_string(), format!("cart_id:{}", cart_id)],
                payload: &json!({
                    "error_message": error.to_string(),
                    "payment_method": payment_method,
                }),
            });
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to checkout cart: {}", error),
            ));
        }
    };

    let analytics_invalidated = match invalidate_analytics_cache(
        &state,
        org_id,
        Some(receipt.user_id),
        "purchase",
        Some("https://app.example.com/checkout/success"),
        false,
        receipt.created_at,
    )
    .await
    {
        Ok(count) => count,
        Err(error) => {
            error!(
                "failed to invalidate analytics cache after checkout: {}",
                error
            );
            0
        }
    };
    let commerce_invalidated = match invalidate_commerce_cache(&state, org_id, Some(cart_id)).await
    {
        Ok(count) => count,
        Err(error) => {
            error!(
                "failed to invalidate commerce cache after checkout: {}",
                error
            );
            0
        }
    };

    if let Some(cart_snapshot) = db
        .get_cart_snapshot(org_id, cart_id)
        .await
        .map_err(|error| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to reload cart after checkout: {}", error),
            )
        })?
    {
        if let Err(error) = cache_cart_snapshot(&state, &cart_snapshot).await {
            error!("failed to cache checked-out cart snapshot: {}", error);
        }
    }

    let invalidated_cache_keys = analytics_invalidated + commerce_invalidated;
    let latency_ns = start.elapsed().as_nanos() as u64;
    state
        .telemetry
        .metrics()
        .record_operation_success("customer_checkout");
    state.telemetry.metrics().record_live_latency_ns(latency_ns);
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::cart_checked_out(),
        org_id: Some(org_id),
        status: "success",
        latency_us: Some(latency_ns as f64 / 1000.0),
        error_type: None,
        extra_tags: vec![
            "role:server".to_string(),
            format!("cart_id:{}", receipt.cart_id),
            format!("order_id:{}", receipt.order_id),
            format!("payment_method:{}", payment_method),
        ],
        payload: &json!({
            "cart_id": receipt.cart_id,
            "order_id": receipt.order_id,
            "payment_id": receipt.payment_id,
            "total_cents": receipt.total_cents,
            "currency": receipt.currency,
        }),
    });

    Ok(Json(CheckoutCartResponse {
        accepted: true,
        organization_id: org_id,
        cart_id: receipt.cart_id,
        order_id: receipt.order_id,
        payment_id: receipt.payment_id,
        total_cents: receipt.total_cents,
        currency: receipt.currency,
        invalidated_cache_keys,
        created_at: receipt.created_at,
    }))
}

async fn track_event_handler(
    State(state): State<AppState>,
    Path(org_id): Path<Uuid>,
    Json(request): Json<TrackEventRequest>,
) -> std::result::Result<Json<TrackEventResponse>, (StatusCode, String)> {
    ensure_known_organization(&state, org_id).await?;

    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "event ingestion requires POSTGRES_ENABLED=true".to_string(),
        )
    })?;

    let event_type = request.event_type.parse::<EventType>().ok().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            format!(
                "invalid event_type '{}'; expected one of: page_view, click, conversion, sign_up, purchase",
                request.event_type
            ),
        )
    })?;

    if let Some(ip_address) = request.ip_address.as_deref() {
        ip_address.parse::<IpAddr>().map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid ip_address '{}': {}", ip_address, error),
            )
        })?;
    }

    let event = Event {
        id: Uuid::new_v4(),
        organization_id: org_id,
        user_id: request.user_id,
        event_type: event_type.as_str().to_string(),
        page_url: request.page_url,
        referrer: request.referrer,
        user_agent: request.user_agent,
        ip_address: request.ip_address,
        properties: request
            .properties
            .unwrap_or_else(|| Value::Object(serde_json::Map::new())),
        created_at: Utc::now(),
    };

    let start = std::time::Instant::now();
    let mut write_span = state.telemetry.tracer().start_span(
        "analytics_server.postgres.insert_events",
        TelemetrySpanKind::Client,
    );
    write_span.enter();
    write_span.set_attribute("db.system", "postgresql");
    write_span.set_attribute("db.operation", "insert_events_batch");
    write_span.set_attribute("analytics.event_type", event_type.as_str());
    write_span.set_attribute("organization.id", org_id.to_string());
    match db.insert_events_batch(std::slice::from_ref(&event)).await {
        Ok(_) => {
            write_span.finish();
        }
        Err(error) => {
            write_span.record_error("query_execution_error");
            write_span.set_attribute("error.message", error.to_string());
            write_span.finish();
            state
                .telemetry
                .metrics()
                .record_operation_error("customer_event_ingest", "query_execution_error");
            let latency_ns = start.elapsed().as_nanos() as u64;
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::event_ingest(),
                org_id: Some(org_id),
                status: "error",
                latency_us: Some(latency_ns as f64 / 1000.0),
                error_type: Some("query_execution_error"),
                extra_tags: vec![
                    "role:server".to_string(),
                    format!("event_type:{}", event_type.as_str()),
                ],
                payload: &json!({
                    "error_message": error.to_string(),
                }),
            });
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to persist event: {}", error),
            ));
        }
    }

    let invalidated_cache_keys = match invalidate_customer_cache(&state, &event).await {
        Ok(count) => count,
        Err(error) => {
            state
                .telemetry
                .metrics()
                .record_operation_error("customer_event_ingest", "cache_invalidation_error");
            error!(
                "Failed to invalidate customer cache after event ingest: {}",
                error
            );
            0
        }
    };

    let latency_ns = start.elapsed().as_nanos() as u64;
    state.telemetry.metrics().record_live_latency_ns(latency_ns);
    state
        .telemetry
        .metrics()
        .record_event_generated(event_type.as_str());
    state
        .telemetry
        .metrics()
        .record_operation_success("customer_event_ingest");
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::event_ingest(),
        org_id: Some(org_id),
        status: "success",
        latency_us: Some(latency_ns as f64 / 1000.0),
        error_type: None,
        extra_tags: vec![
            "role:server".to_string(),
            format!("event_type:{}", event_type.as_str()),
        ],
        payload: &json!({
            "invalidated_cache_keys": invalidated_cache_keys,
            "persisted_to_postgres": true,
        }),
    });

    Ok(Json(TrackEventResponse {
        accepted: true,
        event_id: event.id,
        organization_id: org_id,
        persisted_to_postgres: true,
        invalidated_cache_keys,
        created_at: event.created_at,
    }))
}

async fn ensure_known_organization(
    state: &AppState,
    org_id: Uuid,
) -> std::result::Result<(), (StatusCode, String)> {
    let known = state
        .org_cache
        .get_org_ids()
        .await
        .into_iter()
        .any(|known_org_id| known_org_id == org_id);

    if known {
        Ok(())
    } else {
        Err((
            StatusCode::NOT_FOUND,
            format!("organization {} was not found", org_id),
        ))
    }
}

fn emit_query_observability(
    state: &AppState,
    org_id: Uuid,
    query_type: &'static str,
    cache_hit: bool,
    latency_ns: u64,
    payload: &Value,
) {
    state.telemetry.metrics().record_live_latency_ns(latency_ns);
    state
        .telemetry
        .metrics()
        .record_operation_success("customer_api_query");
    state.telemetry.metrics().record_query_execution(
        query_type,
        latency_ns as f64 / 1_000_000_000.0,
        cache_hit,
    );
    update_customer_query_kpis(state.telemetry.metrics(), query_type, payload);
    state
        .telemetry
        .emit_query_result(query_type, org_id, cache_hit, latency_ns, payload);
}

fn emit_query_error(
    state: &AppState,
    org_id: Uuid,
    query_type: &'static str,
    error_type: &'static str,
    error_message: &str,
    latency_ns: Option<u64>,
) {
    state
        .telemetry
        .metrics()
        .record_operation_error("customer_api_query", error_type);
    state
        .telemetry
        .emit_query_error(query_type, org_id, error_type, error_message, latency_ns);
}

fn update_customer_query_kpis(metrics: &AppMetrics, query_type: &str, payload: &Value) {
    match query_type {
        "analytics_overview_1h" | "analytics_overview_24h" => {
            if let Some(rate) = payload.get("conversion_rate").and_then(Value::as_f64) {
                metrics.update_business_kpi("conversion_rate", rate);
            }
        }
        "event_distribution" => {
            if let Some(total) = payload.get("total").and_then(Value::as_i64) {
                metrics.update_business_kpi("events_last_24h", total as f64);
            }
        }
        "storefront" => {
            if let Some(rate) = payload.get("cart_abandonment_rate").and_then(Value::as_f64) {
                metrics.update_business_kpi("cart_abandonment_rate", rate);
            }
            if let Some(total) = payload.get("carts_total").and_then(Value::as_i64) {
                metrics.update_business_kpi("carts_total", total as f64);
            }
        }
        "catalog" => {
            if let Some(products) = payload.get("products").and_then(Value::as_array) {
                metrics.update_business_kpi("catalog_products", products.len() as f64);
            }
        }
        "cart_detail" => {
            if let Some(item_count) = payload.get("item_count").and_then(Value::as_i64) {
                metrics.update_business_kpi("cart_item_count", item_count as f64);
            }
        }
        _ => {}
    }
}

async fn load_cached_query<T, Loader, LoaderFuture>(
    state: &AppState,
    org_id: Uuid,
    cache_key: String,
    query_type: &'static str,
    ttl_seconds: u64,
    loader: Loader,
) -> std::result::Result<T, (StatusCode, String)>
where
    T: Clone + Serialize + DeserializeOwned,
    Loader: FnOnce() -> LoaderFuture,
    LoaderFuture: Future<Output = AnyResult<T>>,
{
    let start = std::time::Instant::now();
    let mut cache_hit = false;
    let mut value = None;

    if let Some(cache) = state.cache.as_ref() {
        let mut cache_span = state.telemetry.tracer().start_span(
            "analytics_server.redis.cache_get",
            TelemetrySpanKind::Client,
        );
        cache_span.enter();
        cache_span.set_attribute("db.system", "redis");
        cache_span.set_attribute("db.operation", "get");
        cache_span.set_attribute("analytics.query_type", query_type);
        cache_span.set_attribute("organization.id", org_id.to_string());
        cache_span.set_attribute("cache.key", &cache_key);
        let cache_result = cache.get::<T>(&cache_key, state.telemetry.metrics()).await;
        match cache_result {
            Ok(Some(cached_value)) => {
                cache_hit = true;
                cache_span.set_attribute("cache.hit", true);
                value = Some(cached_value);
            }
            Ok(None) => {
                cache_span.set_attribute("cache.hit", false);
            }
            Err(error) => {
                cache_span.record_error("cache_get_error");
                cache_span.set_attribute("error.message", error.to_string());
                emit_query_error(
                    state,
                    org_id,
                    query_type,
                    "cache_get_error",
                    &error.to_string(),
                    None,
                );
            }
        }
        cache_span.finish();
    }

    let value = match value {
        Some(cached_value) => cached_value,
        None => {
            let source_name = if state.db.is_some() {
                "postgres"
            } else {
                "synthetic"
            };
            let span_name = if state.db.is_some() {
                "analytics_server.postgres.load"
            } else {
                "analytics_server.synthetic.load"
            };
            let mut load_span = state
                .telemetry
                .tracer()
                .start_span(span_name, TelemetrySpanKind::Client);
            load_span.enter();
            load_span.set_attribute("analytics.query_type", query_type);
            load_span.set_attribute("organization.id", org_id.to_string());
            load_span.set_attribute("analytics.source", source_name);
            let loaded_value = match loader().await {
                Ok(loaded_value) => {
                    load_span.finish();
                    loaded_value
                }
                Err(error) => {
                    load_span.record_error("query_execution_error");
                    load_span.set_attribute("error.message", error.to_string());
                    load_span.finish();
                    let latency_ns = start.elapsed().as_nanos() as u64;
                    emit_query_error(
                        state,
                        org_id,
                        query_type,
                        "query_execution_error",
                        &error.to_string(),
                        Some(latency_ns),
                    );
                    return Err((
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("failed to load {}: {}", query_type, error),
                    ));
                }
            };

            if let Some(cache) = state.cache.as_ref() {
                let mut cache_set_span = state.telemetry.tracer().start_span(
                    "analytics_server.redis.cache_set",
                    TelemetrySpanKind::Client,
                );
                cache_set_span.enter();
                cache_set_span.set_attribute("db.system", "redis");
                cache_set_span.set_attribute("db.operation", "set");
                cache_set_span.set_attribute("analytics.query_type", query_type);
                cache_set_span.set_attribute("organization.id", org_id.to_string());
                cache_set_span.set_attribute("cache.key", &cache_key);
                cache_set_span.set_attribute("cache.ttl_seconds", ttl_seconds as i64);
                if let Err(error) = cache
                    .set(
                        &cache_key,
                        &loaded_value,
                        ttl_seconds,
                        state.telemetry.metrics(),
                    )
                    .await
                {
                    emit_query_error(
                        state,
                        org_id,
                        query_type,
                        "cache_set_error",
                        &error.to_string(),
                        None,
                    );
                    cache_set_span.record_error("cache_set_error");
                    cache_set_span.set_attribute("error.message", error.to_string());
                }
                cache_set_span.finish();
            }

            loaded_value
        }
    };

    let latency_ns = start.elapsed().as_nanos() as u64;
    if let Ok(payload) = serde_json::to_value(&value) {
        emit_query_observability(state, org_id, query_type, cache_hit, latency_ns, &payload);
    } else {
        state.telemetry.metrics().record_live_latency_ns(latency_ns);
        state
            .telemetry
            .metrics()
            .record_operation_success("customer_api_query");
        state.telemetry.metrics().record_query_execution(
            query_type,
            latency_ns as f64 / 1_000_000_000.0,
            cache_hit,
        );
    }

    Ok(value)
}

async fn load_overview(
    state: &AppState,
    org_id: Uuid,
    hours: u32,
) -> std::result::Result<AnalyticsOverview, (StatusCode, String)> {
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.query.overview",
        TelemetrySpanKind::Internal,
    );
    span.enter();
    span.set_attribute("organization.id", org_id.to_string());
    span.set_attribute("analytics.hours", hours as i64);
    let query_type = if hours <= 1 {
        "analytics_overview_1h"
    } else {
        "analytics_overview_24h"
    };
    let cache_key = state.generator.cache_key_overview(org_id, hours);

    let result = load_cached_query(
        state,
        org_id,
        cache_key,
        query_type,
        OVERVIEW_TTL_SECONDS,
        || async move {
            if let Some(db) = state.db.as_ref() {
                db.get_analytics_overview(org_id, hours as i32).await
            } else {
                Ok(SyntheticDataGenerator::analytics_overview(
                    org_id,
                    hours as i32,
                ))
            }
        },
    )
    .await;
    if let Err((_, message)) = &result {
        span.record_error("query_execution_error");
        span.set_attribute("error.message", message);
    }
    span.finish();
    result
}

async fn load_top_pages(
    state: &AppState,
    org_id: Uuid,
    limit: usize,
) -> std::result::Result<Vec<TopPage>, (StatusCode, String)> {
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.query.top_pages",
        TelemetrySpanKind::Internal,
    );
    span.enter();
    span.set_attribute("organization.id", org_id.to_string());
    span.set_attribute("analytics.limit", limit as i64);

    let result = load_cached_query(
        state,
        org_id,
        state.generator.cache_key_top_pages(org_id, 24),
        "top_pages",
        TOP_PAGES_TTL_SECONDS,
        || async move {
            if let Some(db) = state.db.as_ref() {
                db.get_top_pages(org_id, TOP_PAGES_CACHE_LIMIT as i32).await
            } else {
                Ok(SyntheticDataGenerator::top_pages())
            }
        },
    )
    .await;
    match result {
        Ok(top_pages) => {
            let trimmed = top_pages.into_iter().take(limit).collect();
            span.finish();
            Ok(trimmed)
        }
        Err((status, message)) => {
            span.record_error("query_execution_error");
            span.set_attribute("error.message", &message);
            span.finish();
            Err((status, message))
        }
    }
}

async fn load_hourly_metrics(
    state: &AppState,
    org_id: Uuid,
    hour_offset: u32,
) -> std::result::Result<HourlyMetrics, (StatusCode, String)> {
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.query.hourly_metrics",
        TelemetrySpanKind::Internal,
    );
    span.enter();
    span.set_attribute("organization.id", org_id.to_string());
    span.set_attribute("analytics.hour_offset", hour_offset as i64);
    let hour = Utc::now() - chrono::Duration::hours(hour_offset as i64);
    let cache_key = state.generator.cache_key_hourly(org_id, hour);

    let result = load_cached_query(
        state,
        org_id,
        cache_key,
        "hourly_metrics",
        HOURLY_TTL_SECONDS,
        || async move {
            if let Some(db) = state.db.as_ref() {
                db.get_hourly_metrics(org_id, hour_offset as i32).await
            } else {
                Ok(SyntheticDataGenerator::hourly_metrics(
                    org_id,
                    hour_offset as i32,
                ))
            }
        },
    )
    .await;
    if let Err((_, message)) = &result {
        span.record_error("query_execution_error");
        span.set_attribute("error.message", message);
    }
    span.finish();
    result
}

async fn load_hourly_series(
    state: &AppState,
    org_id: Uuid,
    points: u32,
) -> std::result::Result<Vec<HourlyMetrics>, (StatusCode, String)> {
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.query.hourly_series",
        TelemetrySpanKind::Internal,
    );
    span.enter();
    span.set_attribute("organization.id", org_id.to_string());
    span.set_attribute("analytics.points", points as i64);
    let mut hourly_metrics = Vec::with_capacity(points as usize);
    for hour_offset in (0..points).rev() {
        match load_hourly_metrics(state, org_id, hour_offset).await {
            Ok(metrics) => hourly_metrics.push(metrics),
            Err((status, message)) => {
                span.record_error("query_execution_error");
                span.set_attribute("error.message", &message);
                span.finish();
                return Err((status, message));
            }
        }
    }
    span.finish();
    Ok(hourly_metrics)
}

async fn load_event_distribution(
    state: &AppState,
    org_id: Uuid,
) -> std::result::Result<EventTypeDistribution, (StatusCode, String)> {
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.query.event_distribution",
        TelemetrySpanKind::Internal,
    );
    span.enter();
    span.set_attribute("organization.id", org_id.to_string());
    let result = load_cached_query(
        state,
        org_id,
        state.generator.cache_key_event_distribution(org_id, "24h"),
        "event_distribution",
        EVENT_DISTRIBUTION_TTL_SECONDS,
        || async move {
            if let Some(db) = state.db.as_ref() {
                db.get_event_distribution(org_id).await
            } else {
                Ok(SyntheticDataGenerator::event_distribution(org_id))
            }
        },
    )
    .await;
    if let Err((_, message)) = &result {
        span.record_error("query_execution_error");
        span.set_attribute("error.message", message);
    }
    span.finish();
    result
}

async fn load_catalog(
    state: &AppState,
    org_id: Uuid,
    limit: usize,
) -> std::result::Result<CatalogResponse, (StatusCode, String)> {
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.query.catalog",
        TelemetrySpanKind::Internal,
    );
    span.enter();
    span.set_attribute("organization.id", org_id.to_string());
    span.set_attribute("analytics.limit", limit as i64);

    let result = load_cached_query(
        state,
        org_id,
        state.generator.cache_key_catalog(org_id),
        "catalog",
        CATALOG_TTL_SECONDS,
        || async move {
            if let Some(db) = state.db.as_ref() {
                let products = db
                    .get_catalog_products_detailed(org_id, CATALOG_CACHE_LIMIT as i32)
                    .await?;
                Ok(CatalogResponse {
                    organization_id: org_id,
                    generated_at: Utc::now(),
                    products,
                })
            } else {
                Ok(build_synthetic_catalog_response(state, org_id, CATALOG_CACHE_LIMIT).await)
            }
        },
    )
    .await;

    match result {
        Ok(mut catalog) => {
            catalog.products.truncate(limit);
            span.finish();
            Ok(catalog)
        }
        Err((status, message)) => {
            span.record_error("query_execution_error");
            span.set_attribute("error.message", &message);
            span.finish();
            Err((status, message))
        }
    }
}

async fn load_storefront(
    state: &AppState,
    org_id: Uuid,
) -> std::result::Result<StorefrontResponse, (StatusCode, String)> {
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.query.storefront",
        TelemetrySpanKind::Internal,
    );
    span.enter();
    span.set_attribute("organization.id", org_id.to_string());

    let result = load_cached_query(
        state,
        org_id,
        state.generator.cache_key_storefront(org_id),
        "storefront",
        STOREFRONT_TTL_SECONDS,
        || async move {
            if let Some(db) = state.db.as_ref() {
                let featured_products = db.get_catalog_products_detailed(org_id, 12).await?;
                let top_products_by_revenue = db
                    .get_top_products_by_revenue(org_id, 5)
                    .await?
                    .into_iter()
                    .map(|(product_name, revenue_cents)| StorefrontProductRevenue {
                        product_name,
                        revenue_cents,
                    })
                    .collect();
                let order_summary = db
                    .get_order_summary(org_id)
                    .await?
                    .into_iter()
                    .map(
                        |(status, order_count, total_cents)| StorefrontOrderSummary {
                            status,
                            order_count,
                            total_cents,
                        },
                    )
                    .collect();
                let low_stock_alerts = db
                    .get_inventory_alerts(org_id)
                    .await?
                    .into_iter()
                    .map(|(product_name, quantity_available, reorder_point)| {
                        StorefrontInventoryAlert {
                            product_name,
                            quantity_available,
                            reorder_point,
                        }
                    })
                    .collect();
                let (carts_abandoned, carts_total) = db.get_cart_abandonment_rate(org_id).await?;
                let cart_abandonment_rate = if carts_total > 0 {
                    carts_abandoned as f64 / carts_total as f64 * 100.0
                } else {
                    0.0
                };

                Ok(StorefrontResponse {
                    organization_id: org_id,
                    generated_at: Utc::now(),
                    featured_products,
                    top_products_by_revenue,
                    order_summary,
                    low_stock_alerts,
                    cart_abandonment_rate,
                    carts_abandoned,
                    carts_total,
                })
            } else {
                Ok(build_synthetic_storefront_response(state, org_id).await)
            }
        },
    )
    .await;

    if let Err((_, message)) = &result {
        span.record_error("query_execution_error");
        span.set_attribute("error.message", message);
    }
    span.finish();
    result
}

async fn load_cart_snapshot(
    state: &AppState,
    org_id: Uuid,
    cart_id: Uuid,
) -> std::result::Result<CartSnapshot, (StatusCode, String)> {
    let start = std::time::Instant::now();
    let cache_key = state.generator.cache_key_cart(org_id, cart_id);
    let mut cache_hit = false;

    if let Some(cache) = state.cache.as_ref() {
        let mut cache_span = state
            .telemetry
            .tracer()
            .start_span("analytics_server.redis.cart_get", TelemetrySpanKind::Client);
        cache_span.enter();
        cache_span.set_attribute("db.system", "redis");
        cache_span.set_attribute("db.operation", "get");
        cache_span.set_attribute("analytics.query_type", "cart_detail");
        cache_span.set_attribute("organization.id", org_id.to_string());
        cache_span.set_attribute("cart.id", cart_id.to_string());
        cache_span.set_attribute("cache.key", &cache_key);
        match cache
            .get::<CartSnapshot>(&cache_key, state.telemetry.metrics())
            .await
        {
            Ok(Some(snapshot)) => {
                cache_hit = true;
                cache_span.set_attribute("cache.hit", true);
                cache_span.finish();
                let latency_ns = start.elapsed().as_nanos() as u64;
                if let Ok(payload) = serde_json::to_value(&snapshot) {
                    emit_query_observability(
                        state,
                        org_id,
                        "cart_detail",
                        cache_hit,
                        latency_ns,
                        &payload,
                    );
                }
                return Ok(snapshot);
            }
            Ok(None) => {
                cache_span.set_attribute("cache.hit", false);
            }
            Err(error) => {
                cache_span.record_error("cache_get_error");
                cache_span.set_attribute("error.message", error.to_string());
                emit_query_error(
                    state,
                    org_id,
                    "cart_detail",
                    "cache_get_error",
                    &error.to_string(),
                    None,
                );
            }
        }
        cache_span.finish();
    }

    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "cart reads require POSTGRES_ENABLED=true".to_string(),
        )
    })?;

    let snapshot = match db.get_cart_snapshot(org_id, cart_id).await {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            let latency_ns = start.elapsed().as_nanos() as u64;
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::query("cart_detail"),
                org_id: Some(org_id),
                status: "error",
                latency_us: Some(latency_ns as f64 / 1000.0),
                error_type: Some("validation_error"),
                extra_tags: vec!["role:server".to_string(), format!("cart_id:{}", cart_id)],
                payload: &json!({
                    "error_message": "cart not found",
                }),
            });
            return Err((
                StatusCode::NOT_FOUND,
                format!("cart {} was not found", cart_id),
            ));
        }
        Err(error) => {
            let latency_ns = start.elapsed().as_nanos() as u64;
            emit_query_error(
                state,
                org_id,
                "cart_detail",
                "query_execution_error",
                &error.to_string(),
                Some(latency_ns),
            );
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("failed to load cart {}: {}", cart_id, error),
            ));
        }
    };

    if let Err(error) = cache_cart_snapshot(state, &snapshot).await {
        emit_query_error(
            state,
            org_id,
            "cart_detail",
            "cache_set_error",
            &error.to_string(),
            None,
        );
    }

    let latency_ns = start.elapsed().as_nanos() as u64;
    if let Ok(payload) = serde_json::to_value(&snapshot) {
        emit_query_observability(
            state,
            org_id,
            "cart_detail",
            cache_hit,
            latency_ns,
            &payload,
        );
    }
    Ok(snapshot)
}

async fn build_synthetic_catalog_response(
    state: &AppState,
    org_id: Uuid,
    limit: usize,
) -> CatalogResponse {
    let user_ids = state.org_cache.get_user_ids(org_id).await;
    let commerce =
        SyntheticDataGenerator::commerce_snapshot(state.generator.as_ref(), org_id, &user_ids);
    let mut products = commerce
        .products
        .into_iter()
        .enumerate()
        .map(|(index, product)| CatalogProduct {
            id: product.id,
            organization_id: product.organization_id,
            sku: product.sku,
            name: product.name,
            description: product.description,
            price_cents: product.price_cents,
            compare_at_price_cents: product.compare_at_price_cents,
            currency: product.currency,
            tags: product.tags,
            images: product.images,
            rating_avg: product.rating_avg,
            rating_count: product.rating_count,
            quantity_available: (48_i32.saturating_sub(index as i32 * 7)).max(3),
            is_low_stock: index >= 3,
        })
        .collect::<Vec<_>>();
    products.truncate(limit);

    CatalogResponse {
        organization_id: org_id,
        generated_at: Utc::now(),
        products,
    }
}

async fn build_synthetic_storefront_response(state: &AppState, org_id: Uuid) -> StorefrontResponse {
    let catalog = build_synthetic_catalog_response(state, org_id, 12).await;
    let featured_products = catalog.products.clone();
    let top_products_by_revenue = featured_products
        .iter()
        .enumerate()
        .map(|(index, product)| StorefrontProductRevenue {
            product_name: product.name.clone(),
            revenue_cents: product.price_cents
                * i64::from(18_i32.saturating_sub(index as i32).max(4)),
        })
        .take(5)
        .collect();
    let order_summary = vec![
        StorefrontOrderSummary {
            status: "confirmed".to_string(),
            order_count: 18,
            total_cents: 148_200,
        },
        StorefrontOrderSummary {
            status: "shipped".to_string(),
            order_count: 11,
            total_cents: 91_400,
        },
        StorefrontOrderSummary {
            status: "delivered".to_string(),
            order_count: 42,
            total_cents: 366_900,
        },
    ];
    let low_stock_alerts = featured_products
        .iter()
        .filter(|product| product.is_low_stock)
        .take(3)
        .map(|product| StorefrontInventoryAlert {
            product_name: product.name.clone(),
            quantity_available: product.quantity_available,
            reorder_point: 12,
        })
        .collect();

    StorefrontResponse {
        organization_id: org_id,
        generated_at: Utc::now(),
        featured_products,
        top_products_by_revenue,
        order_summary,
        low_stock_alerts,
        cart_abandonment_rate: 27.5,
        carts_abandoned: 11,
        carts_total: 40,
    }
}

async fn cache_cart_snapshot(state: &AppState, snapshot: &CartSnapshot) -> AnyResult<()> {
    let Some(cache) = state.cache.as_ref() else {
        return Ok(());
    };

    cache
        .set(
            &state
                .generator
                .cache_key_cart(snapshot.organization_id, snapshot.id),
            snapshot,
            CART_TTL_SECONDS,
            state.telemetry.metrics(),
        )
        .await
}

async fn invalidate_analytics_cache(
    state: &AppState,
    org_id: Uuid,
    user_id: Option<Uuid>,
    event_type: &str,
    page_url: Option<&str>,
    referrer_present: bool,
    occurred_at: chrono::DateTime<Utc>,
) -> AnyResult<usize> {
    let Some(cache) = state.cache.as_ref() else {
        return Ok(0);
    };

    let mut keys = vec![
        state.generator.cache_key_overview(org_id, 1),
        state.generator.cache_key_overview(org_id, 6),
        state.generator.cache_key_overview(org_id, 24),
        state.generator.cache_key_overview(org_id, 168),
        state.generator.cache_key_hourly(org_id, occurred_at),
        state.generator.cache_key_top_pages(org_id, 24),
        state.generator.cache_key_event_distribution(org_id, "24h"),
        state.generator.cache_key_realtime(org_id),
    ];

    if let Some(user_id) = user_id {
        keys.push(state.generator.cache_key_user_activity(user_id));
    }
    if let Some(page_url) = page_url {
        keys.push(state.generator.cache_key_page(org_id, page_url));
    }
    if referrer_present {
        keys.push(state.generator.cache_key_referrers(org_id, "24h"));
    }

    match event_type {
        "sign_up" | "conversion" => {
            keys.push(state.generator.cache_key_funnel(org_id, "activation"));
            keys.push(state.generator.cache_key_cohort(org_id, "monthly"));
            keys.push(state.generator.cache_key_marketing(org_id));
        }
        "purchase" => {
            keys.push(state.generator.cache_key_marketing(org_id));
            keys.push(state.generator.cache_key_commerce(org_id));
        }
        _ => {}
    }

    keys.sort();
    keys.dedup();
    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.redis.invalidate",
        TelemetrySpanKind::Client,
    );
    span.enter();
    span.set_attribute("db.system", "redis");
    span.set_attribute("db.operation", "del_batch");
    span.set_attribute("organization.id", org_id.to_string());
    span.set_attribute("analytics.event_type", event_type);
    span.set_attribute("cache.key_count", keys.len() as i64);
    cache.del_batch(&keys, state.telemetry.metrics()).await?;
    span.finish();
    Ok(keys.len())
}

async fn invalidate_commerce_cache(
    state: &AppState,
    org_id: Uuid,
    cart_id: Option<Uuid>,
) -> AnyResult<usize> {
    let Some(cache) = state.cache.as_ref() else {
        return Ok(0);
    };

    let mut keys = vec![
        state.generator.cache_key_storefront(org_id),
        state.generator.cache_key_catalog(org_id),
        state.generator.cache_key_commerce(org_id),
    ];
    if let Some(cart_id) = cart_id {
        keys.push(state.generator.cache_key_cart(org_id, cart_id));
    }

    keys.sort();
    keys.dedup();

    let mut span = state.telemetry.tracer().start_span(
        "analytics_server.redis.invalidate_commerce",
        TelemetrySpanKind::Client,
    );
    span.enter();
    span.set_attribute("db.system", "redis");
    span.set_attribute("db.operation", "del_batch");
    span.set_attribute("organization.id", org_id.to_string());
    span.set_attribute("cache.key_count", keys.len() as i64);
    cache.del_batch(&keys, state.telemetry.metrics()).await?;
    span.finish();
    Ok(keys.len())
}

fn sanitize_payment_method(value: Option<&str>) -> &'static str {
    match value.unwrap_or("credit_card") {
        "credit_card" => "credit_card",
        "debit_card" => "debit_card",
        "bank_transfer" => "bank_transfer",
        "paypal" => "paypal",
        "stripe" => "stripe",
        "crypto" => "crypto",
        "invoice" => "invoice",
        _ => "credit_card",
    }
}

async fn invalidate_customer_cache(state: &AppState, event: &Event) -> AnyResult<usize> {
    invalidate_analytics_cache(
        state,
        event.organization_id,
        event.user_id,
        &event.event_type,
        event.page_url.as_deref(),
        event.referrer.is_some(),
        event.created_at,
    )
    .await
}

async fn start_cache_warmup(state: AppState) {
    let cache = match state.cache {
        Some(c) => c,
        None => return,
    };
    let worker = CacheWarmupWorker::new(
        cache,
        state.telemetry.clone(),
        state.generator.clone(),
        state.org_cache.clone(),
    );

    // Initial bulk population
    tokio::select! {
        _ = sleep(Duration::from_secs(1)) => {}
        _ = state.shutdown.cancelled() => return,
    }
    info!("Starting initial cache population...");
    if let Err(e) = worker.bulk_populate().await {
        error!("Bulk cache population error: {}", e);
    }

    // Periodic refresh (much less frequent since no DB)
    loop {
        tokio::select! {
            _ = sleep(Duration::from_secs(state.config.warmup_interval)) => {}
            _ = state.shutdown.cancelled() => break,
        }
        if let Err(e) = worker.warmup_refresh().await {
            error!("Cache warmup error: {}", e);
        }
    }
}

async fn start_event_simulator(state: AppState, keys_written: Arc<AtomicU64>) {
    let cache = match state.cache {
        Some(c) => c,
        None => return,
    };
    let worker = EventSimulatorWorker::new(
        cache,
        state.telemetry.clone(),
        state.generator.clone(),
        state.controls.clone(),
        state.org_cache.clone(),
        keys_written,
        state.config.redis_target_keys,
    );

    // Wait for cache warmup
    tokio::select! {
        _ = sleep(Duration::from_secs(3)) => {}
        _ = state.shutdown.cancelled() => return,
    }
    let tick = Duration::from_millis(100);
    let mut ticker = tokio::time::interval(tick);
    let mut rate_accumulator = runtime_controls::RateAccumulator::default();

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = state.shutdown.cancelled() => break,
        }
        let ops = rate_accumulator.take_for_tick(state.controls.snapshot().events_per_second, tick);
        if ops == 0 {
            continue;
        }
        if let Err(e) = worker.run_batch(ops).await {
            error!("Event simulator error: {}", e);
        }
    }
}

async fn start_query_simulator(state: AppState) {
    let cache = match state.cache {
        Some(c) => c,
        None => return,
    };
    let worker = QuerySimulatorWorker::new(
        cache,
        state.telemetry.clone(),
        state.generator.clone(),
        state.controls.clone(),
        state.org_cache.clone(),
    );

    // Wait for cache warmup
    tokio::select! {
        _ = sleep(Duration::from_secs(3)) => {}
        _ = state.shutdown.cancelled() => return,
    }

    // Start worker pool
    worker
        .start_worker_pool(
            state.config.organizations,
            state.config.max_workers,
            state.shutdown.clone(),
        )
        .await;

    // Keep task alive
    loop {
        tokio::select! {
            _ = sleep(Duration::from_secs(3600)) => {}
            _ = state.shutdown.cancelled() => break,
        }
    }
}

async fn start_system_monitor(state: AppState) {
    let worker = SystemMonitorWorker::new(
        state.telemetry.clone(),
        state.org_cache.clone(),
        state.controls.clone(),
        state.db.clone(),
    );

    loop {
        if let Err(e) = worker.update_system_metrics().await {
            error!("System monitor error: {}", e);
        }
        tokio::select! {
            _ = sleep(Duration::from_secs(10)) => {}
            _ = state.shutdown.cancelled() => break,
        }
    }
}

async fn start_pg_query_simulator(state: AppState) {
    let db = match state.db {
        Some(db) => db,
        None => return,
    };
    let worker = PgQuerySimulatorWorker::new(
        db,
        state.telemetry.clone(),
        state.generator.clone(),
        state.org_cache.clone(),
    );

    // Wait for DB seeding and cache warmup
    tokio::select! {
        _ = sleep(Duration::from_secs(5)) => {}
        _ = state.shutdown.cancelled() => return,
    }
    info!("Starting PostgreSQL query simulator workers...");

    worker
        .start_worker_pool(state.config.pg_query_workers, state.shutdown.clone())
        .await;

    // Keep task alive
    loop {
        tokio::select! {
            _ = sleep(Duration::from_secs(3600)) => {}
            _ = state.shutdown.cancelled() => break,
        }
    }
}

async fn start_pg_event_writer(state: AppState) {
    let db = match state.db {
        Some(db) => db,
        None => return,
    };
    let worker = PgEventWriterWorker::new(
        db,
        state.telemetry.clone(),
        state.generator.clone(),
        state.org_cache.clone(),
    );

    // Wait for DB seeding and cache warmup
    tokio::select! {
        _ = sleep(Duration::from_secs(5)) => {}
        _ = state.shutdown.cancelled() => return,
    }
    info!("Starting PostgreSQL event writer...");

    loop {
        if let Err(e) = worker.run_batch(state.config.pg_events_per_second).await {
            error!("PG event writer error: {}", e);
        }
        tokio::select! {
            _ = sleep(Duration::from_secs(1)) => {}
            _ = state.shutdown.cancelled() => break,
        }
    }
}
