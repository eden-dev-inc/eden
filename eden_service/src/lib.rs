#![cfg_attr(test, allow(clippy::unwrap_used))]
#![recursion_limit = "512"]
#![allow(
    clippy::too_many_arguments,
    clippy::clone_on_copy,
    clippy::needless_borrow,
    clippy::collapsible_if,
    clippy::redundant_closure,
    clippy::useless_conversion,
    clippy::bind_instead_of_map,
    clippy::needless_return
)]
//! # Eden Service
//!
//! Main HTTP API service for Eve, built on Actix-web.
//!
//! ## Overview
//!
//! `eden_service` is the primary entry point for all HTTP requests to Eve.
//! It provides a REST API for managing organizations, users, endpoints, workflows,
//! and templates with authentication, rate limiting and telemetry.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────┐
//! │       HTTP Client Request            │
//! └────────────────┬─────────────────────┘
//!                  │
//!                  ▼
//! ┌──────────────────────────────────────┐
//! │     Rate Limiting (Governor)         │
//! └────────────────┬─────────────────────┘
//!                  │
//!                  ▼
//! ┌──────────────────────────────────────┐
//! │   Authentication Middleware          │
//! │   - Basic Auth / Bearer Token        │
//! │   - JWT Validation                   │
//! └────────────────┬─────────────────────┘
//!                  │
//!                  ▼
//! ┌──────────────────────────────────────┐
//! │     Metrics Middleware               │
//! │   - Request/response tracking        │
//! │   - OpenTelemetry spans              │
//! └────────────────┬─────────────────────┘
//!                  │
//!                  ▼
//! ┌──────────────────────────────────────┐
//! │        Route Handlers                │
//! │   - Organizations (/org)             │
//! │   - Users (/users)                   │
//! │   - Endpoints (/connect, /endpoint)  │
//! │   - Workflows (/workflow)            │
//! │   - Templates (/template)            │
//! │   - RBAC (/rbac)                     │
//! └────────────────┬─────────────────────┘
//!                  │
//!                  ▼
//! ┌──────────────────────────────────────┐
//! │      Communication Layer             │
//! │   (gRPC to backend services)         │
//! └──────────────────────────────────────┘
//! ```
//!
//! ## Core Features
//!
//! ### Authentication
//!
//! Three authentication methods supported:
//! - **Basic Auth**: Username/password for initial login
//! - **Bearer Token**: JWT tokens for authenticated requests
//! - **Organization Token**: Service-to-service authentication
//!
//! See [`auth`] module for validator implementations.
//!
//! ### Rate Limiting
//!
//! Actix-Governor middleware provides per-IP rate limiting:
//! - Configurable via `EDEN_RATE_LIMIT` environment variable
//! - Set to `0` to disable rate limiting
//! - Default: 100 requests per minute per IP
//!
//! ### OpenAPI Documentation
//!
//! Auto-generated Swagger UI available at:
//! - `/swagger-ui/` - Interactive API documentation
//! - `/api-docs/openapi.json` - OpenAPI 3.0 specification
//!
//! Generated from route handlers using `utoipa` macros.
//!
//! ### Telemetry
//!
//! Comprehensive observability via OpenTelemetry:
//! - **Traces**: Distributed tracing for all requests
//! - **Metrics**: Request counts, latencies, error rates
//! - **Logs**: Structured logging with `env_logger`
//!
//! Metrics exposed via [`MetricsMiddleware`](eden_core::telemetry::MetricsMiddleware).
//!
//! ## API Endpoints
//!
//! ### Organization Management
//!
//! - `POST /create/organization` - Create new organization
//! - `GET /organizations` - List all organizations
//! - `GET /org/{id}` - Get organization details
//! - `PUT /org/{id}` - Update organization
//! - `DELETE /org/{id}` - Delete organization
//!
//! ### User Management
//!
//! - `POST /users` - Create new user
//! - `POST /auth/login` - Authenticate user
//! - `GET /users` - List users in organization
//! - `GET /users/{id}` - Get user details
//! - `PUT /users/{id}` - Update user
//! - `DELETE /users/{id}` - Delete user
//!
//! ### Endpoint Operations
//!
//! - `POST /connect/{db_type}` - Connect new database endpoint
//! - `GET /endpoint/{id}` - Get endpoint configuration
//! - `GET /endpoints` - List all endpoints
//! - `DELETE /disconnect/{id}` - Disconnect endpoint
//! - `POST /endpoint/{id}/execute` - Execute operation on endpoint
//! - `GET /endpoint/{id}/metadata` - Query endpoint metadata
//!
//! ### Workflow Management
//!
//! - `POST /workflow` - Create workflow
//! - `GET /workflow/{id}` - Get workflow definition
//! - `POST /workflow/{id}/execute` - Execute workflow
//! - `PUT /workflow/{id}` - Update workflow
//! - `DELETE /workflow/{id}` - Delete workflow
//!
//! ### Template Management
//!
//! - `POST /template` - Create template
//! - `GET /template/{id}` - Get template
//! - `PUT /template/{id}` - Update template
//! - `DELETE /template/{id}` - Delete template
//!
//! ### RBAC
//!
//! - `POST /rbac/grant` - Grant permission
//! - `DELETE /rbac/revoke` - Revoke permission
//! - `GET /rbac/check` - Check permission
//!
//! ## Error Handling
//!
//! All errors are handled through [`error_handling`] function which:
//! 1. Logs error with code and message
//! 2. Sets OpenTelemetry span status to error
//! 3. Converts [`EpError`](error::EpError) to HTTP response
//!
//! HTTP status codes are automatically mapped from error types.
//!
//! ## Server Startup
//!
//! ```ignore
//! use eden_service::http_server;
//! use actix_governor::GovernorConfigBuilder;
//!
//! #[actix_web::main]
//! async fn main() -> std::io::Result<()> {
//!     // Initialize telemetry, database, etc.
//!
//!     // Configure rate limiting
//!     let governor_conf = GovernorConfigBuilder::default()
//!         .per_second(2)
//!         .burst_size(10)
//!         .finish()
//!         .unwrap_or_default();
//!
//!     // Start HTTP server
//!     http_server(
//!         governor_conf,
//!         metrics_middleware,
//!         database_manager,
//!         template_registry,
//!         bind_address,
//!     ).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Configuration
//!
//! Environment variables:
//! - `EDEN_PORT` - Server port (default: 8000)
//! - `EDEN_JWT_SECRET` - JWT signing secret (base64 encoded)
//! - `EDEN_RATE_LIMIT` - Rate limit in ms per request (0 = disabled)
//! - `RUST_LOG` - Logging level (info, debug, trace)
//! - `EDEN_OTLP_COLLECTOR` - OpenTelemetry collector endpoint
//!
//! ## Testing
//!
//! Test utilities in [`test_utils`] module for integration tests:
//!
//! ```ignore
//! use eden_service::test_utils::create_test_app;
//!
//! #[actix_web::test]
//! async fn test_create_organization() {
//!     let app = create_test_app().await;
//!
//!     let resp = app.post("/create/organization")
//!         .send_json(&org_data)
//!         .await;
//!
//!     assert_eq!(resp.status(), 200);
//! }
//! ```

use crate::auth::{basic_auth_validator, bearer_auth_validator, org_token_validator};
#[cfg(not(embedded_db))]
use crate::comm::endpoints::metadata::MetadataCollector;
use actix_governor::{
    Governor, GovernorConfig, PeerIpKeyExtractor,
    governor::{clock::QuantaInstant, middleware::NoOpMiddleware},
};
use actix_web::web::Data;
use actix_web::{
    App, HttpRequest, HttpResponse, HttpServer,
    middleware::{Logger, from_fn},
    web::{self},
};
use actix_web_httpauth::middleware::HttpAuthentication;
#[cfg(feature = "openapi")]
use apidocs::ApiDocs;
use dashmap::DashMap;
#[cfg(embedded_db)]
use database::db::turso::TursoPool;
#[cfg(not(embedded_db))]
use database::lib::PgConn;
use database::lib::RedisConn;
use database::lib::{ClickhouseConn, DatabaseManager};

/// Type alias for the database manager, feature-gated for embedded vs standard deployment.
#[cfg(not(embedded_db))]
pub type EdenDb = DatabaseManager<RedisConn, PgConn, ClickhouseConn>;
#[cfg(embedded_db)]
pub type EdenDb = DatabaseManager<RedisConn, TursoPool, ClickhouseConn>;
use eden_core::format::cache_uuid::InterlayCacheUuid;
use eden_core::telemetry::MetricsMiddleware;
use eden_core::telemetry::{FastSpan, FastSpanStatus};
use eden_core::telemetry::{TelemetryDurations, TelemetryLabels, TelemetryWrapper};
use eden_core::{error::EpError, request::ServerData};
use eden_logger_internal::{ctx_with_trace, log_error};
use endpoint_core::ep_core::database::schema::interlay::InterlayState;
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use function_name::named;
use std::borrow::Cow;
use std::sync::Arc;
#[cfg(feature = "openapi")]
use utoipa::OpenApi;
#[cfg(feature = "openapi")]
use utoipa_swagger_ui::SwaggerUi;

/// OpenAPI documentation generation and Swagger UI configuration.
pub mod apidocs;

/// Authentication validators for Basic Auth, Bearer Token, and Organization Token.
pub mod auth;

/// HTTP route handlers for all API endpoints (organizations, users, endpoints, workflows, templates, RBAC).
pub mod comm;

/// Service-local configuration loaded from Eden's centralized config.
pub mod config;

/// Serves the embedded Leptos dashboard (static WASM SPA) when built with the
/// `embedded-dashboard` feature.
#[cfg(feature = "embedded-dashboard")]
pub mod webui;

/// JSON fallback for unmatched API routes. This must stay available even when
/// the embedded dashboard module is not compiled.
pub async fn api_not_found(req: HttpRequest) -> HttpResponse {
    HttpResponse::NotFound().json(serde_json::json!({
        "error": "not_found",
        "message": format!("No API route matched {}", req.path()),
    }))
}

pub(crate) mod connection_metrics_aggregation;

/// Shared application state and data structures.
pub mod data;

/// Runtime CPU affinity helpers for the main Tokio runtime used by interlay proxy work.
pub mod runtime_affinity;

pub mod analytics {
    use database::lib::ClickhouseConn;
    use once_cell::sync::Lazy;
    use tokio::sync::watch;

    /// Stub for InfrastructureSnapshotStore when analytics is disabled.
    #[derive(Default)]
    pub(crate) struct InfrastructureSnapshotStore;

    #[allow(dead_code)]
    pub(crate) static INFRASTRUCTURE_SNAPSHOTS: Lazy<InfrastructureSnapshotStore> = Lazy::new(InfrastructureSnapshotStore::default);

    impl InfrastructureSnapshotStore {
        #[allow(dead_code)]
        pub(crate) fn record_started(
            &self,
            _organization_uuid: &str,
            _snapshot_uuid: &str,
            _source_endpoint_uuid: &str,
            _target_count: u32,
        ) {
        }

        #[allow(dead_code)]
        pub(crate) fn record_scheduler_poll(&self, _organization_uuid: &str, _snapshots_due: u32) {}

        #[allow(dead_code)]
        pub(crate) fn record_completed_with_metrics(
            &self,
            _organization_uuid: &str,
            _snapshot_uuid: &str,
            _source_endpoint_uuid: &str,
            _target_count: u32,
            _duration_secs: f64,
            _batches_total: u64,
            _bytes_written_total: u64,
            _target_writes_success: u64,
            _target_writes_failure: u64,
        ) {
        }

        #[allow(dead_code)]
        pub(crate) fn record_failed_with_metrics(
            &self,
            _organization_uuid: &str,
            _snapshot_uuid: &str,
            _source_endpoint_uuid: &str,
            _duration_secs: f64,
            _error_msg: &str,
            _bytes_written_total: u64,
            _target_writes_success: u64,
            _target_writes_failure: u64,
        ) {
        }

        #[allow(dead_code)]
        pub(crate) fn record_failed(
            &self,
            _organization_uuid: &str,
            _snapshot_uuid: &str,
            _source_endpoint_uuid: &str,
            _duration_secs: f64,
            _error_msg: &str,
        ) {
        }
    }

    #[derive(Clone, Default)]
    pub struct AnalyticsState;

    #[derive(Clone, Default)]
    pub(crate) struct AnalyticsStateSnapshot {
        pub(crate) shutdown_tx: Option<watch::Sender<bool>>,
    }

    impl AnalyticsState {
        pub(crate) fn new() -> Self {
            Self
        }

        pub(crate) async fn snapshot(&self) -> AnalyticsStateSnapshot {
            AnalyticsStateSnapshot { shutdown_tx: None }
        }
    }

    pub(crate) struct AnalyticsHandles {
        pub shutdown_tx: watch::Sender<bool>,
    }
    #[allow(dead_code)]
    pub(crate) async fn init_request_analytics(
        _analytics_state: &AnalyticsState,
        _clickhouse_pool: &ClickhouseConn,
        _all_metrics: std::sync::Arc<eden_core::telemetry::AllMetrics>,
        _interlay_endpoints: std::sync::Arc<
            dashmap::DashMap<
                eden_core::format::cache_uuid::InterlayCacheUuid,
                endpoint_core::ep_core::database::schema::interlay::InterlayState,
            >,
        >,
    ) -> Option<AnalyticsHandles> {
        log::warn!("verbose request analytics are unavailable in this build");
        None
    }
}

pub use analytics::AnalyticsState;

pub use eden_gateway as gateway;

/// Actix-web middleware (org-level rate limiting).
pub mod middleware;

/// Org-level bandwidth rate limiting (background ClickHouse poller + shared usage map).
pub mod rate_limiter;

/// Backup restoration at startup
#[cfg(not(embedded_db))]
pub mod backup_restore;

pub mod pipeline;

#[cfg(feature = "llm")]
pub mod seed_skills;

/// User session and API usage tracking for analytics.
pub mod user_sessions;

/// API usage tracking middleware.
#[cfg(feature = "api-usage-tracking")]
pub mod api_usage_middleware;

/// JWT blacklist for revoked sessions.
pub mod jwt_blacklist;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

/// Standardized error handling at the top level of the endpoint request path
#[named]
pub(crate) fn error_handling(e: EpError, span: &mut FastSpan) -> actix_web::Error {
    let ctx = e.merge_with_context(ctx_with_trace!());
    let audience = e.log_audience();

    log_error!(ctx, e.to_string(), audience = audience);

    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });

    e.into()
}

fn http_bind_address(eden_port: u16) -> String {
    #[cfg(embedded_db)]
    {
        let bind_all = std::env::var("EDEN_EMBEDDED_HTTP_BIND_ALL")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);
        let host = if bind_all { "0.0.0.0" } else { "127.0.0.1" };
        format!("{host}:{eden_port}")
    }

    #[cfg(not(embedded_db))]
    {
        format!("0.0.0.0:{eden_port}")
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn http_server(
    governor_conf: GovernorConfig<PeerIpKeyExtractor, NoOpMiddleware<QuantaInstant>>,
    metrics_middleware: MetricsMiddleware,
    all_metrics_data: web::Data<eden_core::telemetry::AllMetrics>,
    node_data: web::Data<eden_core::comm::NodeData>,
    server_data_ref: web::Data<ServerData>,
    engine_service: web::Data<ep_runtime::comp::MyEngineService>,
    template_registry: web::Data<TemplateRegistry>,
    database_manager: web::Data<EdenDb>,
    license_rsa_key: web::Data<data::LicenseRsaPublicKey>,
    eden_port: u16,
) -> Result<(), std::io::Error> {
    // rustls default crypto provider is aws_lc_rs, but it looks like some dependency adds "ring"
    // so we need to explicitly set the provider here - only once per process, so it's at the very top
    // in the main function
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Install the global metrics handle so deep modules (pool managers, etc.)
    // can emit connection-lifecycle events without plumbing AllMetrics through.
    eden_core::telemetry::set_global_metrics(&all_metrics_data.clone().into_inner());

    // Interlay state (includes shutdown channels and abort handles).
    let interlay_endpoints: Data<DashMap<InterlayCacheUuid, InterlayState>> = Data::new(DashMap::new());
    // Per-interlay mutex to serialize concurrent mutations (PATCH, DELETE, START, STOP).
    let interlay_locks: Data<DashMap<InterlayCacheUuid, Arc<tokio::sync::Mutex<()>>>> = Data::new(DashMap::new());

    // Initialize CDC pipeline manager for CDC-mode snapshots.
    let cdc_manager = Data::new(pipeline::manager::CdcManager::new());
    #[cfg(feature = "llm")]
    let proxy_gateway_state = Data::new(comm::llm::proxy::ProxyGatewayState::new());
    #[cfg(feature = "llm")]
    let agent_gateway_state = Data::new(eden_gateway::agent::AgentGatewayState::default());
    #[cfg(feature = "llm")]
    let llm_service_config = eden_config::services().llm.clone();
    #[cfg(feature = "llm")]
    let hydrated_gateway_api_keys = comm::llm::proxy::hydrate_gateway_api_keys(
        &database_manager,
        proxy_gateway_state.get_ref(),
        &mut TelemetryWrapper::new(
            all_metrics_data.clone().into_inner(),
            TelemetryLabels::new(node_data.uuid()),
            TelemetryDurations::default(),
        ),
    )
    .await
    .map_err(std::io::Error::other)?;
    #[cfg(feature = "llm")]
    tracing::info!(gateway_api_key_count = hydrated_gateway_api_keys, "Hydrated LLM gateway API keys");
    #[cfg(feature = "llm")]
    if let Some(path) = llm_service_config.gateway_snapshot_publish_path.as_deref().map(str::trim).filter(|path| !path.is_empty())
        && let Some(config) =
            comm::llm::proxy::GatewaySnapshotPublisherConfig::new(path, llm_service_config.gateway_snapshot_publish_interval_secs)
    {
        comm::llm::proxy::spawn_gateway_snapshot_publisher(proxy_gateway_state.clone().into_inner(), config);
    }

    // Start background OpenRouter pricing cache refresh for LLM cost estimation.
    #[cfg(feature = "llm")]
    endpoint_core::llm_core::pricing::spawn_pricing_refresh_task();

    // Initialize analytics pipeline first so we can pass the health publisher to metadata collection.
    let analytics_state = Data::new(analytics::AnalyticsState::new());
    let connection_metrics_stream_manager = Data::new(Arc::new(comm::connection_metrics::ConnectionMetricsStreamManager::new()));
    let connection_metrics_stream_runner = connection_metrics_stream_manager.get_ref().clone();
    tokio::spawn(async move {
        connection_metrics_stream_runner.run().await;
    });
    let analytics_handles = analytics::init_request_analytics(
        analytics_state.get_ref(),
        database_manager.clickhouse_pool(),
        all_metrics_data.clone().into_inner(),
        interlay_endpoints.clone().into_inner(),
    )
    .await;
    let _analytics_shutdown = analytics_handles.as_ref().map(|h| h.shutdown_tx.clone());
    let analytics_state_for_shutdown = analytics_state.clone();

    // Start background metadata collection tasks.
    #[cfg(not(embedded_db))]
    MetadataCollector::sync_endpoints(
        node_data.uuid().to_owned(),
        engine_service.clone().into_inner(),
        database_manager.clone().into_inner(),
        all_metrics_data.clone(),
    )
    .await;

    // Capture the main tokio runtime handle before actix-web takes over.
    // Actix workers run single-threaded runtimes; spawning interlay tasks
    // there pins all proxy work to one core. Using this handle ensures proxy
    // TCP listeners and per-connection tasks run on the multi-threaded main runtime.
    let proxy_runtime: Data<tokio::runtime::Handle> = Data::new(tokio::runtime::Handle::current());

    // Thread-per-core proxy shard runtimes. Each shard owns one OS thread,
    // a current_thread tokio runtime + LocalSet, and (eventually) all
    // connection / multiplexer state for the connections routed to it.
    // Constructed once at startup and shared via actix Data so the
    // interlay listener path can dispatch accepted connections through it.
    //
    // Sized by EDEN_PROXY_SHARD_COUNT when present, otherwise by available
    // CPU parallelism. K_CHOICE=2 enables shuffle-sharding +
    // power-of-two-choices dispatch (Brooker's SFQ recommendation): each
    // connection maps to two shards, each batch is dispatched to whichever
    // has shorter inflight queue.
    let shard_config = runtime_affinity::proxy_shard_runtime_config();
    let shard_router: Data<comm::interlays::shard::ShardRouter> =
        Data::new(comm::interlays::shard::ShardRouter::start(shard_config.shard_count, shard_config.k_choice));
    tracing::info!(
        shard_count = shard_config.shard_count,
        k_choice = shard_config.k_choice,
        shard_count_source = shard_config.source.as_str(),
        "Started proxy shard runtimes"
    );

    // reconnect relays, log errors but continue even if there are errors
    #[cfg(not(embedded_db))]
    comm::interlays::post::reconnect_interlays(
        engine_service.clone(),
        database_manager.clone(),
        interlay_endpoints.clone(),
        node_data.uuid().clone(),
        &proxy_runtime,
        Some(shard_router.clone().into_inner()),
        &mut TelemetryWrapper::new(
            all_metrics_data.clone().into_inner(),
            TelemetryLabels::new(node_data.uuid()),
            TelemetryDurations::default(),
        ),
    )
    .await
    .map_err(std::io::Error::other)?;

    let result = HttpServer::new(move || {
        let cors = actix_cors::Cors::permissive().allow_any_origin();
        let basic_auth_middleware = HttpAuthentication::basic(basic_auth_validator);
        let bearer_auth_middleware = HttpAuthentication::bearer(bearer_auth_validator);
        let relay_org_token_middleware = HttpAuthentication::bearer(org_token_validator);

        let app = App::new().wrap(Governor::new(&governor_conf)).wrap(metrics_middleware.clone());
        #[cfg(feature = "api-usage-tracking")]
        let app = app.wrap(api_usage_middleware::ApiUsageTracking);
        let app = app
            .wrap(Logger::default())
            .wrap(cors)
            .app_data(all_metrics_data.clone())
            .app_data(node_data.clone())
            .app_data(server_data_ref.clone())
            .app_data(engine_service.clone())
            .app_data(template_registry.clone())
            .app_data(database_manager.clone())
            .app_data(engine_service.clone())
            .app_data(template_registry.clone())
            .app_data(database_manager.clone())
            .app_data(interlay_endpoints.clone())
            .app_data(interlay_locks.clone())
            .app_data(analytics_state.clone())
            .app_data(connection_metrics_stream_manager.clone())
            .app_data(proxy_runtime.clone())
            .app_data(shard_router.clone())
            .app_data(license_rsa_key.clone())
            .app_data(cdc_manager.clone());

        #[cfg(feature = "llm")]
        let app = app.app_data(proxy_gateway_state.clone()).app_data(agent_gateway_state.clone());

        // When the dashboard is embedded, `/` serves the SPA shell via the
        // default_service fallback below instead of this empty health route.
        #[cfg(not(feature = "embedded-dashboard"))]
        let app = app.route("", web::get().to(HttpResponse::Ok));
        #[cfg(feature = "openapi")]
        let app = app
            .route("/api-docs/openapi.json", web::get().to(apidocs::serve_openapi_json))
            .service(SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-docs/openapi.json", ApiDocs::openapi()));
        #[cfg(feature = "llm")]
        let app = app.service(
            web::scope("/proxy/v1")
                .route("/chat/completions", web::post().to(comm::llm::proxy::chat_completions))
                .route("/responses", web::post().to(comm::llm::proxy::responses))
                .route("/models", web::get().to(comm::llm::proxy::list_models))
                .default_service(web::to(api_not_found)),
        );

        #[cfg(not(embedded_db))]
        let app = app.service(
            web::scope("/api/v1/backups")
                .wrap(HttpAuthentication::bearer(bearer_auth_validator))
                .route("", web::get().to(comm::backups::list::list))
                .route("", web::post().to(comm::backups::post::post))
                .route("/{timestamp}/download", web::get().to(comm::backups::download::download))
                .route("/{timestamp}", web::delete().to(comm::backups::delete::delete))
                .default_service(web::to(api_not_found)),
        );

        let app = app.service(
            web::scope("/api/v1")
                    // Public Routes
                    .service(
                        web::scope("/new")
                            .wrap(relay_org_token_middleware)
                            .route("", web::post().to(comm::organization::post::post))
                    )
                    .service(
                        web::scope("/help")
                            .route("", web::get().to(comm::lib::help))
                    )
                    .service(
                        web::scope("/auth")
                            // .service(
                            //     web::scope("/password")
                            //         .route("/reset", web::post().to(comm::auth::password::reset)),
                            // )
                            // basic auth validation is used only for login
                            .service(
                                web::scope("/login")
                                    .wrap(basic_auth_middleware.clone())
                                    .route("", web::post().to(comm::auth::login::login))
                            )
                            .service(
                                web::scope("/refresh")
                                    .wrap(bearer_auth_middleware.clone())
                                    .route("", web::post().to(comm::auth::login::refresh)) // same handler as auth/login, just with bearer middleware
                            )
                            .service(
                                web::scope("/robots/login")
                                    .route("", web::post().to(comm::auth::robot_login::robot_login))
                            )
                    )
                    // Protected Routes
                    .service({
                        let scope = web::scope("/analytics")
                            .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                            .wrap(bearer_auth_middleware.clone())
                            .route("/status", web::get().to(comm::analytics::status))
                            .route("/overview", web::get().to(comm::analytics::overview))
                            .route("/enable", web::post().to(comm::analytics::enable))
                            .route("/disable", web::post().to(comm::analytics::disable))
                            .route("/dashboard", web::get().to(comm::analytics::dashboard))
                            .route("/connections", web::get().to(comm::connection_metrics::connections))
                            .route("/connections/stream", web::get().to(comm::connection_metrics::stream_connections))
                            .route("/telemetry", web::get().to(comm::telemetry_analytics::export))
                            .route("/telemetry/{signal}", web::get().to(comm::telemetry_analytics::export_signal))
                            .route("/clickhouse", web::get().to(comm::telemetry_analytics::export))
                            .route("/clickhouse/{signal}", web::get().to(comm::telemetry_analytics::export_signal))
                            // Compress only the batch series payload (columnar JSON);
                            // scoped here so it never buffers the SSE stream routes.
                            .service(
                                web::scope("/series")
                                    .wrap(actix_web::middleware::Compress::default())
                                    .route("", web::get().to(comm::telemetry_series::series)),
                            );
                        #[cfg(feature = "stream")]
                        let scope = scope
                            .route("/stream", web::get().to(comm::analytics_stream::stream_sse))
                            .route("/stream/queries", web::get().to(comm::analytics_stream::query_stream_sse))
                            .route("/stream/queries/capture", web::get().to(comm::analytics_stream::capture_all_status))
                            .route("/stream/queries/capture", web::post().to(comm::analytics_stream::activate_capture_all))
                            .route("/stream/queries/capture", web::delete().to(comm::analytics_stream::deactivate_capture_all));
                        scope
                    })
                    .service(
                        {
                            let scope = web::scope("/organizations")
                                .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                                .wrap(bearer_auth_middleware.clone())
                                .route("", web::get().to(comm::organization::get::get))
                                .route("", web::patch().to(comm::organization::patch::patch))
                                .route("", web::delete().to(comm::organization::delete::delete))
                                .route("/rate-limit", web::get().to(comm::organization::rate_limit::get_rate_limit));
                            #[cfg(not(embedded_db))]
                            let scope = scope
                                .route("/export", web::post().to(comm::org_transfer::export::post_export))
                                .route("/import", web::post().to(comm::org_transfer::import::post_import));
                            scope
                        },
                    )
                    .service(
                        // route for all Identity and Access Management (IAM) functionality
                        web::scope("/iam")
                            .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                            .wrap(bearer_auth_middleware.clone())
                            .service(
                                web::scope("/access").service(
                                    web::scope("/endpoints")
                                        .route("/{endpoint}", web::get().to(comm::iam::endpoints::access::get)),
                                ),
                            )
                            .service(
                                web::scope("/security").service(
                                    web::scope("/endpoints")
                                        .route("/{endpoint}", web::get().to(comm::iam::endpoints::get::get)),
                                ),
                            )
                            .service(
                                web::scope("/control")
                                    .service(
                                        web::scope("/subjects/{subject}")
                                            .route("", web::get().to(comm::iam::rbac::subjects::get::get))
                                            .route("", web::delete().to(comm::iam::rbac::subjects::delete::delete))
                                            .route("/endpoints", web::get().to(comm::iam::rbac::subjects::endpoints::get))
                                            .route("/organizations", web::get().to(comm::iam::rbac::subjects::organizations::get))
                                            .route("/templates", web::get().to(comm::iam::rbac::subjects::templates::get))
                                            .route("/workflows", web::get().to(comm::iam::rbac::subjects::workflows::get))
                                    )
                                    .service(
                                        web::scope("/organizations")
                                            .route("", web::get().to(comm::iam::rbac::organizations::get::get))
                                            .route("", web::delete().to(comm::iam::rbac::organizations::delete::delete))
                                            .service(
                                                web::scope("/subjects")
                                                    .route("/{subject}", web::get().to(comm::iam::rbac::organizations::subjects::get::get))
                                                    .route("/{subject}", web::put().to(comm::iam::control::put_organization_subject))
                                                    .route(
                                                        "/{subject}",
                                                        web::delete().to(comm::iam::rbac::organizations::subjects::delete::delete),
                                                    ),
                                            ),
                                    )
                                    .service(
                                        web::scope("/endpoints")
                                            .route("/{endpoint}", web::get().to(comm::iam::rbac::endpoints::get::get))
                                            .route("/{endpoint}", web::delete().to(comm::iam::rbac::endpoints::delete::delete))
                                            .route(
                                                "/{endpoint}/subjects/{subject}",
                                                web::get().to(comm::iam::rbac::endpoints::subjects::get::get),
                                            )
                                            .route(
                                                "/{endpoint}/subjects/{subject}",
                                                web::put().to(comm::iam::control::put_endpoint_subject),
                                            )
                                            .route(
                                                "/{endpoint}/subjects/{subject}",
                                                web::delete().to(comm::iam::rbac::endpoints::subjects::delete::delete),
                                            ),
                                    )
                                    .service(
                                        web::scope("/templates")
                                            .route("/{template}", web::get().to(comm::iam::rbac::templates::get::get))
                                            .route("/{template}", web::delete().to(comm::iam::rbac::templates::delete::delete))
                                            .route(
                                                "/{template}/subjects/{subject}",
                                                web::get().to(comm::iam::rbac::templates::subjects::get::get),
                                            )
                                            .route(
                                                "/{template}/subjects/{subject}",
                                                web::put().to(comm::iam::control::put_template_subject),
                                            )
                                            .route(
                                                "/{template}/subjects/{subject}",
                                                web::delete().to(comm::iam::rbac::templates::subjects::delete::delete),
                                            ),
                                    )
                                    .service(
                                        web::scope("/workflows")
                                            .route("/{workflow}", web::get().to(comm::iam::rbac::workflows::get::get))
                                            .route("/{workflow}", web::delete().to(comm::iam::rbac::workflows::delete::delete))
                                            .route(
                                                "/{workflow}/subjects/{subject}",
                                                web::get().to(comm::iam::rbac::workflows::subjects::get::get),
                                            )
                                            .route(
                                                "/{workflow}/subjects/{subject}",
                                                web::put().to(comm::iam::control::put_workflow_subject),
                                            )
                                            .route(
                                                "/{workflow}/subjects/{subject}",
                                                web::delete().to(comm::iam::rbac::workflows::subjects::delete::delete),
                                            ),
                                    ),
                            )
                            .service(
                                web::scope("/data")
                                    .service(
                                        web::scope("/endpoints")
                                            .route("/{endpoint}", web::get().to(comm::iam::data::get_endpoint))
                                            .route("/{endpoint}", web::delete().to(comm::iam::data::delete_endpoint))
                                            .route(
                                                "/{endpoint}/subjects/{subject}",
                                                web::get().to(comm::iam::data::get_endpoint_subject),
                                            )
                                            .route(
                                                "/{endpoint}/subjects/{subject}",
                                                web::put().to(comm::iam::data::put_endpoint_subject),
                                            )
                                            .route(
                                                "/{endpoint}/subjects/{subject}",
                                                web::delete().to(comm::iam::data::delete_endpoint_subject),
                                            ),
                                    )
                                    .service(
                                        web::scope("/subjects")
                                            .route("/{subject}/endpoints", web::get().to(comm::iam::data::get_subject_endpoints))
                                            .route("/{subject}", web::delete().to(comm::iam::data::delete_subject)),
                                    ),
                            )
                            .service(
                                web::scope("/els").service(
                                    web::scope("/endpoints")
                                        .route("/{endpoint}/policies", web::post().to(comm::endpoints::els::create_policy))
                                        .route("/{endpoint}/validate", web::post().to(comm::endpoints::els::validate_policy))
                                        .route("/{endpoint}/policies", web::get().to(comm::endpoints::els::list_policies))
                                        .route("/{endpoint}/policies", web::delete().to(comm::endpoints::els::delete_all_policies))
                                        .route("/{endpoint}/policies/{policy_uuid}", web::get().to(comm::endpoints::els::get_policy))
                                        .route("/{endpoint}/policies/{policy_uuid}", web::put().to(comm::endpoints::els::update_policy))
                                        .route(
                                            "/{endpoint}/policies/{policy_uuid}",
                                            web::delete().to(comm::endpoints::els::delete_policy),
                                        )
                                        .route(
                                            "/{endpoint}/policies/{policy_uuid}/versions",
                                            web::post().to(comm::endpoints::els::create_version),
                                        )
                                        .route(
                                            "/{endpoint}/policies/{policy_uuid}/versions",
                                            web::get().to(comm::endpoints::els::list_versions),
                                        )
                                        .route(
                                            "/{endpoint}/policies/{policy_uuid}/versions/active",
                                            web::get().to(comm::endpoints::els::get_active_version),
                                        )
                                        .route(
                                            "/{endpoint}/policies/{policy_uuid}/versions/{version}",
                                            web::get().to(comm::endpoints::els::get_version),
                                        )
                                        .route(
                                            "/{endpoint}/policies/{policy_uuid}/pointer",
                                            web::get().to(comm::endpoints::els::get_pointer),
                                        )
                                        .route(
                                            "/{endpoint}/policies/{policy_uuid}/versions/{version}/promote",
                                            web::post().to(comm::endpoints::els::promote_version),
                                        )
                                        .route(
                                            "/{endpoint}/policies/{policy_uuid}/versions/{version}/reject",
                                            web::post().to(comm::endpoints::els::reject_version),
                                        )
                                        .route(
                                            "/{endpoint}/policies/{policy_uuid}/versions/{version}/rollback",
                                            web::post().to(comm::endpoints::els::rollback_version),
                                        )
                                        .route("/{endpoint}/users", web::put().to(comm::endpoints::els::assign_users))
                                        .route("/{endpoint}/users", web::get().to(comm::endpoints::els::list_user_assignments))
                                        .route("/{endpoint}/users/unassign", web::post().to(comm::endpoints::els::unassign_users))
                                        .route("/{endpoint}/users", web::delete().to(comm::endpoints::els::unassign_all))
                                        .route("/{endpoint}/users/{user_uuid}", web::put().to(comm::endpoints::els::assign_user))
                                        .route("/{endpoint}/users/{user_uuid}", web::get().to(comm::endpoints::els::get_user_policy))
                                        .route(
                                            "/{endpoint}/users/{user_uuid}/refresh",
                                            web::post().to(comm::endpoints::els::refresh_user_policy),
                                        )
                                        .route("/{endpoint}/users/{user_uuid}", web::delete().to(comm::endpoints::els::unassign_user)),
                                ),
                            )
                            .service(
                                web::scope("/humans")
                                    .route("", web::post().to(comm::iam::users::post::post))
                                    .route("", web::get().to(comm::iam::users::get::get_all))
                                    .route("/me", web::get().to(comm::iam::users::me::get_me))
                                    .route("/me", web::patch().to(comm::iam::users::me::patch_me))
                                    .route("/me/analytics-prefs", web::get().to(comm::iam::users::analytics_prefs::get))
                                    .route("/me/analytics-prefs", web::put().to(comm::iam::users::analytics_prefs::put))
                                    .route("/{human}", web::get().to(comm::iam::users::get::get))
                                    .route(
                                        "/{human}",
                                        web::patch().to(comm::iam::users::patch::patch),
                                    )
                                    .route(
                                        "/{human}",
                                        web::delete().to(comm::iam::users::delete::delete),
                                    ),
                            )
                            .service(
                                web::scope("/agents")
                                    .route("", web::get().to(comm::iam::robots::list::list))
                                    .route("", web::post().to(comm::iam::robots::post::post))
                                    .route("/{agent}", web::get().to(comm::iam::robots::get::get))
                                    .route("/{agent}", web::patch().to(comm::iam::robots::patch::patch))
                                    .route("/{agent}/rotate-key", web::post().to(comm::iam::robots::rotate_key::post))
                                    .route("/{agent}", web::delete().to(comm::iam::robots::delete::delete)),
                            )
                            .service(
                                web::scope("/sessions")
                                    .route("", web::get().to(comm::iam::sessions::list_sessions))
                                    .route("/history", web::get().to(comm::iam::sessions::get_session_history))
                                    .route("/revoke-others", web::post().to(comm::iam::sessions::revoke_others))
                                    .route("/revoke-all", web::post().to(comm::iam::sessions::revoke_all)),
                            )
                            .route("/usage", web::get().to(comm::iam::sessions::get_api_usage)),
                    )
                    .service(
                        web::scope("/notifications")
                            .wrap(bearer_auth_middleware.clone())
                            .route("", web::get().to(comm::notifications::list_user_notifications))
                            .route("/read-all", web::post().to(comm::notifications::mark_all_user_notifications_read))
                            .route("/{notification_id}/read", web::post().to(comm::notifications::mark_user_notification_read)),
                    )
                    .service(
                        web::scope("/apis")
                            .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                            .wrap(bearer_auth_middleware.clone())
                            .route("", web::post().to(comm::apis::post::post))
                            .route("", web::get().to(comm::apis::get::get_all))
                            .route("/updated", web::get().to(comm::apis::get::get_all_updated))
                            .route("/{api}", web::get().to(comm::apis::get::get))
                            .route("/{api}", web::patch().to(comm::apis::patch::patch))
                            .route(
                                "/{api}",
                                web::delete().to(comm::apis::delete::delete),
                            )
                            .route("/{api}", web::post().to(comm::apis::run::run))
                    )
                    .service(
                        web::scope("/interlays")
                            .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                            .wrap(bearer_auth_middleware.clone())
                            .route("", web::get().to(comm::interlays::get::get_all))
                            .route("", web::post().to(comm::interlays::post::post))
                            .route("/updated", web::get().to(comm::interlays::get::get_all_updated))
                            .route("/{interlay}", web::get().to(comm::interlays::get::get))
                            .route(
                                "/{interlay}/analysis/timeseries",
                                web::get().to(comm::interlays::analysis_timeseries::get_analysis_timeseries),
                            )
                            .route("/{interlay}", web::patch().to(comm::interlays::patch::patch))
                            .route("/{interlay}", web::delete().to(comm::interlays::delete::delete))
                            .route("/{interlay}/start", web::post().to(comm::interlays::start::start))
                            .route("/{interlay}/stop", web::post().to(comm::interlays::stop::stop))
                    )
                    .service(
                        web::scope("/endpoint-groups")
                            .wrap(bearer_auth_middleware.clone())
                            .route("", web::get().to(comm::endpoint_groups::get::get_all))
                            .route("", web::post().to(comm::endpoint_groups::post::post))
                            .route("/{group}", web::get().to(comm::endpoint_groups::get::get))
                            .route("/{group}", web::patch().to(comm::endpoint_groups::patch::patch))
                            .route("/{group}", web::delete().to(comm::endpoint_groups::delete::delete))
                            .route("/{group}/members", web::post().to(comm::endpoint_groups::members::add_member))
                            .route("/{group}/members/{endpoint}", web::delete().to(comm::endpoint_groups::members::remove_member))
                    )
                    .service({
                        #[cfg(feature = "llm")]
                        {
                            web::scope("/llm")
                                .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                                .wrap(bearer_auth_middleware.clone())
                                .route(
                                    "/agent-gateway/connections",
                                    web::post().to(comm::llm::agent_gateway::register_connection),
                                )
                                .route(
                                    "/agent-gateway/connections",
                                    web::get().to(comm::llm::agent_gateway::list_connections),
                                )
                                .route(
                                    "/agent-gateway/connections/{session_id}/heartbeat",
                                    web::post().to(comm::llm::agent_gateway::heartbeat_connection),
                                )
                                .route(
                                    "/agent-gateway/connections/{session_id}/usage",
                                    web::post().to(comm::llm::agent_gateway::record_usage),
                                )
                                .route(
                                    "/agent-gateway/connections/{session_id}/drain",
                                    web::post().to(comm::llm::agent_gateway::mark_connection_draining),
                                )
                                .route(
                                    "/agent-gateway/connections/{session_id}",
                                    web::delete().to(comm::llm::agent_gateway::disconnect_connection),
                                )
                                .route(
                                    "/agent-gateway/usage",
                                    web::get().to(comm::llm::agent_gateway::list_usage),
                                )
                                .route(
                                    "/agent-gateway/agents/{agent_id}/route",
                                    web::get().to(comm::llm::agent_gateway::route_to_agent),
                                )
                                .route(
                                    "/endpoints",
                                    web::get().to(comm::llm::endpoints::list_chat_endpoints),
                                )
                                .route(
                                    "/credentials",
                                    web::get().to(comm::llm::credentials::list),
                                )
                                .route(
                                    "/credentials",
                                    web::post().to(comm::llm::credentials::create),
                                )
                                .route(
                                    "/credentials/{credential_id}",
                                    web::patch().to(comm::llm::credentials::update),
                                )
                                .route(
                                    "/credentials/{credential_id}",
                                    web::delete().to(comm::llm::credentials::delete),
                                )
                                .route(
                                    "/gateway/requests",
                                    web::get().to(comm::llm::requests::gateway_requests),
                                )
                                .route(
                                    "/cost/timeseries",
                                    web::get().to(comm::llm::cost::cost_timeseries),
                                )
                                .route(
                                    "/pricing",
                                    web::get().to(comm::llm::cost::pricing),
                                )
                                .route(
                                    "/gateway_snapshot",
                                    web::get().to(comm::llm::proxy::gateway_control_plane_snapshot),
                                )
                                .route(
                                    "/api_keys",
                                    web::get().to(comm::llm::proxy::list_api_keys),
                                )
                                .route(
                                    "/api_keys",
                                    web::post().to(comm::llm::proxy::create_api_key),
                                )
                                .route(
                                    "/api_keys/{key_id}",
                                    web::patch().to(comm::llm::proxy::update_api_key),
                                )
                                .route(
                                    "/api_keys/{key_id}",
                                    web::delete().to(comm::llm::proxy::delete_api_key),
                                )
                                .route(
                                    "/pii_dictionary",
                                    web::get().to(comm::llm::proxy::get_org_pii_dictionary),
                                )
                                .route(
                                    "/pii_dictionary",
                                    web::put().to(comm::llm::proxy::update_org_pii_dictionary),
                                )
                        }
                        #[cfg(not(feature = "llm"))]
                        {
                            web::scope("/llm")
                        }
                    })
                    .service(
                        web::scope("/functions")
                            .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                            .wrap(bearer_auth_middleware.clone())
                            .route("", web::post().to(comm::functions::post))
                            .route(
                                "/{endpoint}/invoke",
                                web::post().to(comm::functions::invoke),
                            )
                    )
                    .service(web::scope("/admin/llm"))
                    .service(
                        web::scope("/endpoints")
                            .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                            .wrap(bearer_auth_middleware.clone())
                            .route("", web::get().to(comm::endpoints::list::list))
                            .route("/updated", web::get().to(comm::endpoints::list::list_updated))
                            .route(
                                "/google_workspace/oauth/exchange",
                                web::post().to(comm::endpoints::google_workspace_oauth::exchange),
                            )
                            .route("", web::post().to(comm::endpoints::post::post))
                            .route("/{endpoint}", web::get().to(comm::endpoints::get::get))
                            .route(
                                "/{endpoint}",
                                web::patch().to(comm::endpoints::patch::patch),
                            )
                            .route(
                                "/{endpoint}",
                                web::delete().to(comm::endpoints::delete::delete),
                            )
                            .route(
                                "/{endpoint}/read",
                                web::post().to(comm::endpoints::read::read),
                            )
                            .route(
                                "/{endpoint}/write",
                                web::post().to(comm::endpoints::write::write),
                            )
                            .route(
                                "/{endpoint}/transaction",
                                web::post().to(comm::endpoints::transaction::transaction),
                            )
                            .route(
                                "/{endpoint}/metadata",
                                web::get().to(comm::endpoints::metadata::metadata),
                            )
                            .route(
                                "/{endpoint}/metadata/collect",
                                web::post().to(comm::endpoints::metadata::metadata_collect),
                            )
                            .route(
                                "/{endpoint}/metadata/collectors",
                                web::get().to(comm::endpoints::metadata::metadata_collectors),
                            )
                            .route(
                                "/{endpoint}/metadata/cache",
                                web::get().to(comm::endpoints::metadata::metadata_cache),
                            )
                            .route(
                                "/{endpoint}/metadata/history",
                                web::get().to(comm::endpoints::metadata::metadata_history),
                            ),
                    )
                    .service(
                        web::scope("/json")
                            .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                            .wrap(bearer_auth_middleware.clone())
                            .route("/flatten", web::post().to(comm::json::flatten::flatten))
                            .route("/map", web::post().to(comm::json::map::map))
                            .route("/parse", web::post().to(comm::json::parse::parse))
                            .route("/reduce", web::post().to(comm::json::reduce::reduce))
                            .route(
                                "/unflatten",
                                web::post().to(comm::json::unflatten::unflatten),
                            ),
                    )
                    .service(
                        web::scope("/templates")
                            .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                            .wrap(bearer_auth_middleware.clone())
                            .route("", web::post().to(comm::templates::post::post))
                            .route("", web::get().to(comm::templates::get::get_all))
                            .route("/updated", web::get().to(comm::templates::get::get_all_updated))
                            .route("/{template}", web::get().to(comm::templates::get::get))
                            .route(
                                "/{template}",
                                web::patch().to(comm::templates::patch::patch),
                            )
                            .route(
                                "/{template}",
                                web::delete().to(comm::templates::delete::delete),
                            )
                            .route("/{template}", web::post().to(comm::templates::run::run))
                            .route(
                                "/{template}/render",
                                web::post().to(comm::templates::render::render),
                            ),
                    )
                    .service({
                        #[cfg(not(embedded_db))]
                        {
                            web::scope("/snapshots")
                                .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                                .wrap(bearer_auth_middleware.clone())
                                .route("", web::get().to(comm::snapshots::get::get_all))
                                .route("", web::post().to(comm::snapshots::post::post))
                                .route("/{snapshot}", web::get().to(comm::snapshots::get::get))
                                .route("/{snapshot}", web::delete().to(comm::snapshots::delete::delete))
                                .route("/{snapshot}/status", web::get().to(comm::snapshots::status::status))
                        }
                        #[cfg(embedded_db)]
                        { web::scope("/snapshots") }
                    })
                    .service({
                        #[cfg(not(embedded_db))]
                        {
                            web::scope("/pipelines")
                                .wrap(bearer_auth_middleware.clone())
                                .route("", web::get().to(comm::pipelines::get::get_all))
                                .route("", web::post().to(comm::pipelines::post::post))
                                .route("/{pipeline}", web::get().to(comm::pipelines::get::get))
                                .route("/{pipeline}", web::delete().to(comm::pipelines::delete::delete))
                                .route("/{pipeline}/status", web::get().to(comm::pipelines::status::status))
                                .route("/{pipeline}/run", web::post().to(comm::pipelines::run::run))
                                .route("/{pipeline}/pause", web::post().to(comm::pipelines::pause::pause))
                        }
                        #[cfg(embedded_db)]
                        { web::scope("/pipelines") }
                    })
                    .service(
                        web::scope("/workflows")
                            .wrap(from_fn(middleware::org_rate_limit::org_rate_limit))
                            .wrap(bearer_auth_middleware.clone())
                            .route("", web::post().to(comm::workflows::post::post))
                            .route("/{workflow}", web::get().to(comm::workflows::get::get))
                            .route(
                                "/{workflow}",
                                web::patch().to(comm::workflows::patch::patch),
                            )
                            .route(
                                "/{workflow}",
                                web::delete().to(comm::workflows::delete::delete),
                            ),
                    )
                    .default_service(web::to(api_not_found)),
        );

        // Embedded Leptos dashboard: any route not matched by an API scope above
        // (assets, SPA deep links, `/`) falls through to the SPA shell.
        #[cfg(feature = "embedded-dashboard")]
        let app = app.default_service(web::to(webui::serve));

        app
    })
    .keep_alive(std::time::Duration::from_secs(15))
    .client_request_timeout(std::time::Duration::from_secs(30))
    .client_disconnect_timeout(std::time::Duration::from_secs(5))
    .bind(http_bind_address(eden_port))?
    .run()
    .await;

    // Shutdown analytics pipeline gracefully
    if let Some(shutdown_tx) = analytics_state_for_shutdown.get_ref().snapshot().await.shutdown_tx {
        let _ = shutdown_tx.send(true);
    }
    #[cfg(feature = "llm")]
    {
        endpoint_core::llm_core::analytics::clear_llm_operation_sender();
        endpoint_core::llm_core::pricing::clear_llm_price_snapshot_sender();
    }

    result
}

#[cfg(test)]
mod tests {
    use super::http_bind_address;
    #[cfg(embedded_db)]
    use std::sync::{Mutex, OnceLock};

    #[cfg(embedded_db)]
    fn bind_env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[cfg(embedded_db)]
    fn with_embedded_bind_all_env(value: Option<&str>, assert_fn: impl FnOnce()) {
        let _guard = bind_env_lock().lock().expect("embedded HTTP bind env lock poisoned");
        let previous = std::env::var_os("EDEN_EMBEDDED_HTTP_BIND_ALL");

        unsafe {
            match value {
                Some(value) => std::env::set_var("EDEN_EMBEDDED_HTTP_BIND_ALL", value),
                None => std::env::remove_var("EDEN_EMBEDDED_HTTP_BIND_ALL"),
            }
        }

        assert_fn();

        unsafe {
            match previous {
                Some(previous) => std::env::set_var("EDEN_EMBEDDED_HTTP_BIND_ALL", previous),
                None => std::env::remove_var("EDEN_EMBEDDED_HTTP_BIND_ALL"),
            }
        }
    }

    #[cfg(embedded_db)]
    #[test]
    fn embedded_db_http_server_binds_to_loopback() {
        with_embedded_bind_all_env(None, || {
            assert_eq!(http_bind_address(6366), "127.0.0.1:6366");
        });
    }

    #[cfg(embedded_db)]
    #[test]
    fn embedded_db_http_server_can_bind_all_for_container_benchmarks() {
        with_embedded_bind_all_env(Some("true"), || {
            assert_eq!(http_bind_address(6366), "0.0.0.0:6366");
        });
    }

    #[cfg(not(embedded_db))]
    #[test]
    fn standard_http_server_binds_to_all_interfaces() {
        assert_eq!(http_bind_address(6366), "0.0.0.0:6366");
    }
}
