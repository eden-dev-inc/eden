#![cfg(external_db)]
#![allow(dead_code)]

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::time::Duration;

use actix_governor::GovernorConfig;
use actix_web::{App, HttpServer, middleware::from_fn, web};
use actix_web_httpauth::middleware::HttpAuthentication;
use dashmap::DashMap;
use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::methods::insert::InsertMethod;
use database::methods::insert::eden_node::InsertEdenNode;
use eden_core::auth::Jwt;
use eden_core::comm::NodeData;
use eden_core::format::EdenNodeId;
use eden_core::format::EdenNodeUuid;
use eden_core::format::cache_id::EdenNodeCacheId;
use eden_core::format::cache_uuid::{EdenNodeCacheUuid, InterlayCacheUuid};
use eden_core::request::ServerData;
use eden_core::telemetry::labels::TelemetryLabels;
use eden_core::telemetry::{MetricsMiddleware, TelemetryDurations, setup_metrics};
use eden_service::auth::{basic_auth_validator, bearer_auth_validator, org_token_validator};
use eden_service::runtime_affinity;
use endpoint_core::ep_core::database::schema::eden_node::EdenNodeSchema;
use endpoint_core::ep_core::database::schema::interlay::InterlayState;
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use ep_runtime::comp::MyEngineService;
use std::sync::{Mutex, OnceLock};
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ContainerAsync;
use tokio::runtime;

/// Test configuration stored thread-safely
#[derive(Clone, Debug)]
pub struct TestConfig {
    pub server_port: u16,
    pub org_id: String,
    pub postgres_conn: String,
    pub redis_conn: String,
    pub clickhouse_url: String,
    pub control_plane_db_path: Option<String>,
}

/// Global thread ID to TestConfig mapping (initialized once)
fn get_test_configs() -> &'static DashMap<std::thread::ThreadId, TestConfig> {
    static TEST_CONFIGS: OnceLock<DashMap<std::thread::ThreadId, TestConfig>> = OnceLock::new();
    TEST_CONFIGS.get_or_init(DashMap::new)
}

/// Last-ready port signalled by the container thread; used as a hint for TestConfig fallback
fn get_last_ready_port() -> &'static Mutex<Option<u16>> {
    static LAST_READY_PORT: OnceLock<Mutex<Option<u16>>> = OnceLock::new();
    LAST_READY_PORT.get_or_init(|| Mutex::new(None))
}

impl TestConfig {
    /// Initialize test config for this thread
    pub fn init(
        port: u16,
        org_id: String,
        postgres_conn: String,
        redis_conn: String,
        clickhouse_url: String,
        control_plane_db_path: Option<String>,
    ) {
        let config = TestConfig {
            server_port: port,
            org_id,
            postgres_conn,
            redis_conn,
            clickhouse_url,
            control_plane_db_path,
        };
        let thread_id = std::thread::current().id();
        get_test_configs().insert(thread_id, config);
    }

    /// Get the current test config
    pub fn get() -> Option<TestConfig> {
        let thread_id = std::thread::current().id();
        // Only use the exact thread ID match - no fallback
        // This ensures each test uses its own isolated server
        get_test_configs().get(&thread_id).map(|entry| entry.clone())
    }

    /// Get server port, default to 8000 if not configured
    pub fn get_port() -> u16 {
        Self::get().map(|c| c.server_port).unwrap_or(8000)
    }

    /// Get organization ID, default to "TestOrg" if not configured
    pub fn get_org_id() -> String {
        Self::get().map(|c| c.org_id).unwrap_or_else(|| "TestOrg".to_string())
    }

    /// Get the external PostgreSQL endpoint target used by HTTP integration tests.
    pub fn get_postgres_conn() -> String {
        Self::get().map(|c| c.postgres_conn).unwrap_or_else(|| "postgresql://postgres:postgres@localhost:5433".to_string())
    }

    /// Get the external Redis endpoint target used by HTTP integration tests.
    pub fn get_redis_conn() -> String {
        Self::get().map(|c| c.redis_conn).unwrap_or_else(|| "redis://127.0.0.1:6379".to_string())
    }

    /// Get the ClickHouse URL used by the analytics layer in tests.
    pub fn get_clickhouse_url() -> String {
        Self::get().map(|c| c.clickhouse_url).unwrap_or_else(|| "http://127.0.0.1:8123".to_string())
    }

    /// Get the embedded-db control-plane database path when the service is not backed by Postgres.
    pub fn get_control_plane_db_path() -> Option<String> {
        Self::get().and_then(|c| c.control_plane_db_path)
    }

    /// Clean up config for current thread (useful for cleanup)
    #[allow(dead_code)]
    pub fn cleanup() {
        let thread_id = std::thread::current().id();
        get_test_configs().remove(&thread_id);
    }
}

/// Find an available port for the test server
pub fn find_available_port() -> Result<u16, Box<dyn std::error::Error>> {
    use std::net::TcpListener;

    // Bind to port 0 to let the OS assign an available port
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    Ok(addr.port())
}

/// Find an available port that passes interlay validation.
pub fn find_available_interlay_port() -> Result<u16, Box<dyn std::error::Error>> {
    use std::net::TcpListener;

    static ALLOCATED_INTERLAY_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
    let allocated_ports = ALLOCATED_INTERLAY_PORTS.get_or_init(|| Mutex::new(HashSet::new()));

    for _ in 0..128 {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        if port < 1024 {
            continue;
        }

        let mut allocated =
            allocated_ports.lock().map_err(|e| std::io::Error::other(format!("Allocated interlay port registry poisoned: {e}")))?;
        if allocated.insert(port) {
            drop(listener);
            return Ok(port);
        }
    }

    Err(std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "Failed to find an available non-privileged interlay port").into())
}

pub enum TestContainer {
    Local,
    Redis(ContainerAsync<Redis>),
    Postgres(ContainerAsync<Postgres>),
    Clickhouse(ContainerAsync<ClickHouse>),
}

impl TestContainer {
    pub async fn stop(&self) -> std::io::Result<()> {
        match self {
            Self::Local => Ok(()),
            Self::Redis(container) => container.stop().await.map_err(|e| std::io::Error::other(e.to_string())),
            Self::Postgres(container) => container.stop().await.map_err(|e| std::io::Error::other(e.to_string())),
            Self::Clickhouse(container) => container.stop().await.map_err(|e| std::io::Error::other(e.to_string())),
        }
    }
}

trait IntoTestContainer {
    fn into_test_container(self) -> TestContainer;
}

impl IntoTestContainer for () {
    fn into_test_container(self) -> TestContainer {
        TestContainer::Local
    }
}

impl IntoTestContainer for ContainerAsync<Redis> {
    fn into_test_container(self) -> TestContainer {
        TestContainer::Redis(self)
    }
}

impl IntoTestContainer for ContainerAsync<Postgres> {
    fn into_test_container(self) -> TestContainer {
        TestContainer::Postgres(self)
    }
}

impl IntoTestContainer for ContainerAsync<ClickHouse> {
    fn into_test_container(self) -> TestContainer {
        TestContainer::Clickhouse(self)
    }
}

pub struct DBContainers {
    pub redis: Arc<TestContainer>,
    pub redis_conn: String,
    pub postgres: Arc<TestContainer>,
    pub postgres_conn: String,
    pub clickhouse: Arc<TestContainer>,
    pub clickhouse_url: String,
    pub control_plane_db_path: Option<String>,
    /// The Eden node UUID inserted by the first test server.  All subsequent test servers that
    /// share these containers must reuse this UUID so the org-creation handler can find the node.
    pub eden_node_uuid: EdenNodeUuid,
}
impl DBContainers {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        create_test_containers().await
    }

    /// Wait for Redis to be ready by attempting a connection
    async fn wait_for_redis(redis_url: &str) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(30);
        let mut interval = Duration::from_millis(100);

        loop {
            if redis::Client::open(redis_url).and_then(|c| c.get_connection()).is_ok() {
                log::info!("Redis is ready");
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err("Redis failed to start within timeout".into());
            }

            tokio::time::sleep(interval).await;
            // Exponential backoff: 100ms -> 200ms -> 500ms max
            interval = std::cmp::min(interval.mul_f32(2.0), Duration::from_millis(500));
        }
    }

    /// Wait for PostgreSQL to be ready by attempting a connection
    async fn wait_for_postgres(pg_url: &str) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(30);
        let mut interval = Duration::from_millis(100);

        loop {
            match tokio_postgres::connect(pg_url, tokio_postgres::tls::NoTls).await {
                Ok((_client, connection)) => {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("Postgres connection error: {}", e);
                        }
                    });
                    log::info!("PostgreSQL is ready");
                    return Ok(());
                }
                Err(_) => {
                    if start.elapsed() > timeout {
                        return Err(format!("PostgreSQL failed to start within timeout at {}", pg_url).into());
                    }

                    tokio::time::sleep(interval).await;
                    // Exponential backoff: 100ms -> 200ms -> 500ms max
                    interval = std::cmp::min(interval.mul_f32(2.0), Duration::from_millis(500));
                }
            }
        }
    }
}

async fn create_test_containers() -> Result<DBContainers, Box<dyn std::error::Error>> {
    use database::test_utils::database_test_utils::{create_clickhouse, create_postgres, create_redis};

    let (redis_container, redis_conn) = create_redis().await;
    let (pg_container, postgres_conn) = create_postgres().await;
    let (clickhouse_container, clickhouse_url) = create_clickhouse().await;
    let control_plane_db_path = if cfg!(embedded_db) {
        Some(format!("/tmp/eden_http_test_{}.db", uuid::Uuid::new_v4()))
    } else {
        None
    };

    DBContainers::wait_for_redis(&redis_conn).await?;
    DBContainers::wait_for_postgres(&postgres_conn).await?;

    Ok(DBContainers {
        redis: Arc::new(redis_container.into_test_container()),
        postgres: Arc::new(pg_container.into_test_container()),
        clickhouse: Arc::new(clickhouse_container.into_test_container()),
        redis_conn,
        postgres_conn,
        clickhouse_url,
        control_plane_db_path,
        eden_node_uuid: EdenNodeUuid::new_uuid(),
    })
}

async fn create_test_database_manager(
    redis_conn: &str,
    postgres_conn: &str,
    clickhouse_url: &str,
    control_plane_db_path: Option<&str>,
    jwt: Jwt,
) -> DatabaseManager<RedisConn, PgConn, ClickhouseConn> {
    cfg_if::cfg_if! {
        if #[cfg(embedded_db)] {
            let _ = postgres_conn;
            let db_path = control_plane_db_path.expect("embedded_db test harness requires a control-plane db path");
            database::test_utils::database_test_utils::build_database_manager(redis_conn, db_path, clickhouse_url, Some(jwt)).await
        } else {
            let _ = control_plane_db_path;
            database::test_utils::database_test_utils::build_database_manager(redis_conn, postgres_conn, clickhouse_url, Some(jwt)).await
        }
    }
}

/// Global container pool - initialized once and reused across all tests
fn get_global_containers() -> &'static Arc<tokio::sync::Mutex<Option<Arc<DBContainers>>>> {
    static GLOBAL_CONTAINERS: OnceLock<Arc<tokio::sync::Mutex<Option<Arc<DBContainers>>>>> = OnceLock::new();
    GLOBAL_CONTAINERS.get_or_init(|| Arc::new(tokio::sync::Mutex::new(None)))
}

/// Get or initialize database containers, reusing a single shared set across all tests in the
/// process.  The first caller starts the containers; subsequent callers receive the same `Arc`.
async fn get_shared_containers() -> Result<Arc<DBContainers>, Box<dyn std::error::Error>> {
    let global = get_global_containers();
    let mut guard = global.lock().await;

    if let Some(containers) = guard.as_ref() {
        log::info!("Reusing existing database containers");
        return Ok(Arc::clone(containers));
    }

    log::info!("Creating shared database containers for this test process");
    let containers = Arc::new(create_test_containers().await?);
    *guard = Some(Arc::clone(&containers));
    Ok(containers)
}

/// Serializes `DatabaseManager::new()` across all concurrently-starting test servers.
/// Without this, 10 servers sharing one Postgres all race to create the same tables.
fn get_schema_init_lock() -> &'static Arc<tokio::sync::Mutex<()>> {
    static LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
}

async fn start_http_server(
    redis_conn: &str,
    postgres_conn: &str,
    clickhouse_url: &str,
    control_plane_db_path: Option<&str>,
    new_org_token: Option<String>,
    server_port: u16,
    eden_node_uuid: EdenNodeUuid,
) {
    println!("[Server] Starting minimal HTTP server for testing");
    println!("[Server] External Redis target: {}", redis_conn);
    println!("[Server] External Postgres target: {}", postgres_conn);
    println!("[Server] ClickHouse URL: {}", clickhouse_url);

    // Install Rustls crypto provider
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let jwt = Jwt::new(b"bXlfdWx0cmFfc2VjdXJlX2p3dF9zZWNyZXQK", 3600);
    println!("[Server] Creating DatabaseManager...");
    let database_manager: web::Data<DatabaseManager<RedisConn, PgConn, ClickhouseConn>> = {
        // Hold the schema-init lock only for the duration of DatabaseManager::new().
        // This prevents concurrent DDL races when multiple test servers share one Postgres.
        let _schema_guard = get_schema_init_lock().lock().await;
        let mgr = create_test_database_manager(redis_conn, postgres_conn, clickhouse_url, control_plane_db_path, jwt).await;
        drop(_schema_guard);
        web::Data::new(mgr)
    };
    println!("[Server] DatabaseManager created successfully");

    // Setup metrics and telemetry FIRST
    let all_metrics = setup_metrics("http://localhost:9999", "").unwrap_or_else(|e| {
        eprintln!("Failed to setup metrics: {}", e);
        panic!("Metrics setup failed")
    });
    let all_metrics_data = web::Data::new(all_metrics);
    let _metrics_middleware = MetricsMiddleware::default();

    // Create NodeData for the organization handler.  When containers are shared across tests,
    // all servers must use the same eden_node_uuid so the DB lookup in the org handler succeeds.
    let eden_node_id = EdenNodeId::from("eden-node");
    let node_data = web::Data::new(NodeData::new(eden_node_id.clone(), eden_node_uuid.clone()));

    // Insert the eden node into the database with retries to handle transient DB pool timeouts
    println!("[Server] Inserting eden node into database...");
    let mut inserted = false;
    let max_attempts = 8usize;
    for attempt in 1..=max_attempts {
        let eden_node_schema = EdenNodeSchema::new("eden-node".to_string(), eden_node_uuid.clone(), vec![], serde_json::json!(""));
        let insert_eden_node = InsertEdenNode::new(eden_node_schema);

        match <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            EdenNodeSchema,
            EdenNodeCacheUuid,
            EdenNodeCacheId,
            InsertEdenNode,
        >>::insert(
            &database_manager,
            insert_eden_node,
            &mut eden_core::telemetry::TelemetryWrapper::new(
                all_metrics_data.clone().into_inner(),
                TelemetryLabels::new(&eden_node_uuid),
                TelemetryDurations::default(),
            ),
        )
        .await
        {
            Ok(_) => {
                println!("[Server] Eden node inserted successfully (attempt {})", attempt);
                inserted = true;
                break;
            }
            Err(e) => {
                log::warn!("[Server] Eden node insert attempt {} failed: {}", attempt, e);
                if attempt == max_attempts {
                    eprintln!("[Server] Failed to insert eden node after {} attempts: {}", max_attempts, e);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }

    if !inserted {
        // Shouldn't reach here because we return on final failure, but keep guard
        eprintln!("[Server] Eden node insert did not complete; aborting server start");
        return;
    }

    // Setup server data for telemetry extractor fallback
    let _server_data = web::Data::new(ServerData {
        engine_url: "http://localhost:8000".to_string(),
        public_key: EdenNodeUuid::new_uuid(),
        new_org_token: new_org_token.clone(),
        tools_service_timeout_secs: None,
        internal_llm: None,
    });

    // Create engine service
    let engine_service = web::Data::new(MyEngineService::default());

    // Create template registry
    let template_registry = web::Data::new(TemplateRegistry::new());

    // Create interlay state storage
    let interlay_endpoints: web::Data<DashMap<InterlayCacheUuid, InterlayState>> = web::Data::new(DashMap::new());

    // Per-interlay mutex to serialize concurrent mutations (PATCH, DELETE, START, STOP)
    let interlay_locks: web::Data<DashMap<InterlayCacheUuid, Arc<tokio::sync::Mutex<()>>>> = web::Data::new(DashMap::new());

    // Capture the main tokio runtime handle for proxy operations
    // This ensures interlay TCP listeners run on the multi-threaded runtime
    let proxy_runtime: web::Data<tokio::runtime::Handle> = web::Data::new(tokio::runtime::Handle::current());

    // Match production interlay startup by providing shard runtimes for proxy dispatch.
    let shard_config = runtime_affinity::proxy_shard_runtime_config();
    let shard_router: web::Data<eden_service::comm::interlays::shard::ShardRouter> = web::Data::new(
        eden_service::comm::interlays::shard::ShardRouter::start(shard_config.shard_count, shard_config.k_choice),
    );

    // Create license RSA key (None for testing - no license validation)
    let license_rsa_key: web::Data<eden_service::data::LicenseRsaPublicKey> = web::Data::new(eden_service::data::LicenseRsaPublicKey(None));

    // Use the port passed as a parameter
    let relay_port = server_port;
    println!("[Server] Starting minimal HTTP server on port {}...", relay_port);

    let _governor_conf = GovernorConfig::default();
    let all_metrics = setup_metrics("http://localhost:9999", "").expect("Failed to setup metrics");
    let all_metrics_data = web::Data::new(all_metrics);
    let metrics_middleware = MetricsMiddleware::default();

    let server_data = ServerData {
        engine_url: String::default(), // engine is not a service anymore
        public_key: eden_node_uuid.clone(),
        new_org_token,
        tools_service_timeout_secs: None,
        internal_llm: None,
    };
    let server_data_ref = web::Data::new(server_data);

    // spawn the server
    let _result = HttpServer::new(move || {
        let cors = actix_cors::Cors::permissive().allow_any_origin();
        let basic_auth_middleware = HttpAuthentication::basic(basic_auth_validator);
        let bearer_auth_middleware = HttpAuthentication::bearer(bearer_auth_validator);
        let relay_org_token_middleware = HttpAuthentication::bearer(org_token_validator);
        let analytics_state = eden_service::analytics::AnalyticsState;

        // Build app incrementally so #[cfg] gates on app_data work without breaking
        // the method chain type inference.
        let app = App::new()
            .wrap(cors)
            .wrap(actix_web::middleware::Logger::default())
            .wrap(metrics_middleware.clone())
            .app_data(database_manager.clone())
            .app_data(node_data.clone())
            .app_data(all_metrics_data.clone())
            .app_data(server_data_ref.clone())
            .app_data(engine_service.clone())
            .app_data(template_registry.clone())
            .app_data(interlay_endpoints.clone())
            .app_data(interlay_locks.clone())
            .app_data(proxy_runtime.clone())
            .app_data(shard_router.clone())
            .app_data(license_rsa_key.clone())
            .app_data(actix_web::web::Data::new(analytics_state));

        // Build /api/v1 scope
        let v1_scope = web::scope("/api/v1")
            .service(
                web::scope("/new")
                    .wrap(relay_org_token_middleware)
                    .route("", web::post().to(eden_service::comm::organization::post::post)),
            )
            .service(
                web::scope("/auth")
                    .service(
                        web::scope("/login")
                            .wrap(basic_auth_middleware.clone())
                            .route("", web::post().to(eden_service::comm::auth::login::login)),
                    )
                    .service(
                        web::scope("/refresh")
                            .wrap(bearer_auth_middleware.clone())
                            .route("", web::post().to(eden_service::comm::auth::login::refresh)),
                    ),
            )
            .service(
                web::scope("/iam")
                    .wrap(bearer_auth_middleware.clone())
                    .service(
                        web::scope("/humans")
                            .route("", web::post().to(eden_service::comm::iam::users::post::post))
                            .route("", web::get().to(eden_service::comm::iam::users::get::get_all))
                            .route("/me", web::get().to(eden_service::comm::iam::users::me::get_me))
                            .route("/me", web::patch().to(eden_service::comm::iam::users::me::patch_me))
                            .route("/{human}", web::get().to(eden_service::comm::iam::users::get::get))
                            .route("/{human}", web::patch().to(eden_service::comm::iam::users::patch::patch))
                            .route("/{human}", web::delete().to(eden_service::comm::iam::users::delete::delete)),
                    )
                    .service(
                        web::scope("/agents")
                            .route("", web::get().to(eden_service::comm::iam::robots::list::list))
                            .route("", web::post().to(eden_service::comm::iam::robots::post::post))
                            .route("/{agent}", web::get().to(eden_service::comm::iam::robots::get::get))
                            .route("/{agent}", web::patch().to(eden_service::comm::iam::robots::patch::patch))
                            .route("/{agent}/rotate-key", web::post().to(eden_service::comm::iam::robots::rotate_key::post))
                            .route("/{agent}", web::delete().to(eden_service::comm::iam::robots::delete::delete)),
                    )
                    .service(
                        web::scope("/sessions")
                            .route("", web::get().to(eden_service::comm::iam::sessions::list_sessions))
                            .route("/history", web::get().to(eden_service::comm::iam::sessions::get_session_history))
                            .route("/revoke-others", web::post().to(eden_service::comm::iam::sessions::revoke_others))
                            .route("/revoke-all", web::post().to(eden_service::comm::iam::sessions::revoke_all)),
                    )
                    .route("/usage", web::get().to(eden_service::comm::iam::sessions::get_api_usage))
                    .service(
                        web::scope("/data/endpoints")
                            .route("/{endpoint}/subjects/{subject}", web::put().to(eden_service::comm::iam::data::put_endpoint_subject)),
                    )
                    .service(
                        web::scope("/control")
                            .service(
                                web::scope("/subjects/{subject}")
                                    .route("", web::get().to(eden_service::comm::iam::rbac::subjects::get::get))
                                    .route("", web::delete().to(eden_service::comm::iam::rbac::subjects::delete::delete))
                                    .route("/endpoints", web::get().to(eden_service::comm::iam::rbac::subjects::endpoints::get))
                                    .route("/organizations", web::get().to(eden_service::comm::iam::rbac::subjects::organizations::get))
                                    .route("/templates", web::get().to(eden_service::comm::iam::rbac::subjects::templates::get))
                                    .route("/workflows", web::get().to(eden_service::comm::iam::rbac::subjects::workflows::get)),
                            )
                            .service(
                                web::scope("/organizations")
                                    .route("", web::get().to(eden_service::comm::iam::rbac::organizations::get::get))
                                    .route("", web::delete().to(eden_service::comm::iam::rbac::organizations::delete::delete))
                                    .service(
                                        web::scope("/subjects")
                                            .route(
                                                "/{subject}",
                                                web::get().to(eden_service::comm::iam::rbac::organizations::subjects::get::get),
                                            )
                                            .route("/{subject}", web::put().to(eden_service::comm::iam::control::put_organization_subject))
                                            .route(
                                                "/{subject}",
                                                web::delete().to(eden_service::comm::iam::rbac::organizations::subjects::delete::delete),
                                            ),
                                    ),
                            )
                            .service(
                                web::scope("/endpoints")
                                    .route("/{endpoint}", web::get().to(eden_service::comm::iam::rbac::endpoints::get::get))
                                    .route("/{endpoint}", web::delete().to(eden_service::comm::iam::rbac::endpoints::delete::delete))
                                    .route(
                                        "/{endpoint}/subjects/{subject}",
                                        web::get().to(eden_service::comm::iam::rbac::endpoints::subjects::get::get),
                                    )
                                    .route(
                                        "/{endpoint}/subjects/{subject}",
                                        web::put().to(eden_service::comm::iam::control::put_endpoint_subject),
                                    )
                                    .route(
                                        "/{endpoint}/subjects/{subject}",
                                        web::delete().to(eden_service::comm::iam::rbac::endpoints::subjects::delete::delete),
                                    ),
                            )
                            .service(
                                web::scope("/templates")
                                    .route("/{template}", web::get().to(eden_service::comm::iam::rbac::templates::get::get))
                                    .route("/{template}", web::delete().to(eden_service::comm::iam::rbac::templates::delete::delete))
                                    .route(
                                        "/{template}/subjects/{subject}",
                                        web::get().to(eden_service::comm::iam::rbac::templates::subjects::get::get),
                                    )
                                    .route(
                                        "/{template}/subjects/{subject}",
                                        web::put().to(eden_service::comm::iam::control::put_template_subject),
                                    )
                                    .route(
                                        "/{template}/subjects/{subject}",
                                        web::delete().to(eden_service::comm::iam::rbac::templates::subjects::delete::delete),
                                    ),
                            )
                            .service(
                                web::scope("/workflows")
                                    .route("/{workflow}", web::get().to(eden_service::comm::iam::rbac::workflows::get::get))
                                    .route("/{workflow}", web::delete().to(eden_service::comm::iam::rbac::workflows::delete::delete))
                                    .route(
                                        "/{workflow}/subjects/{subject}",
                                        web::get().to(eden_service::comm::iam::rbac::workflows::subjects::get::get),
                                    )
                                    .route(
                                        "/{workflow}/subjects/{subject}",
                                        web::put().to(eden_service::comm::iam::control::put_workflow_subject),
                                    )
                                    .route(
                                        "/{workflow}/subjects/{subject}",
                                        web::delete().to(eden_service::comm::iam::rbac::workflows::subjects::delete::delete),
                                    ),
                            ),
                    ),
            )
            .service(
                web::scope("/endpoints")
                    .wrap(bearer_auth_middleware.clone())
                    .route("", web::post().to(eden_service::comm::endpoints::post::post))
                    .route("/{endpoint}/read", web::post().to(eden_service::comm::endpoints::read::read))
                    .route("/{endpoint}/write", web::post().to(eden_service::comm::endpoints::write::write))
                    .route("/{endpoint}/transaction", web::post().to(eden_service::comm::endpoints::transaction::transaction)),
            )
            .service(
                web::scope("/apis")
                    .wrap(bearer_auth_middleware.clone())
                    .route("", web::post().to(eden_service::comm::apis::post::post))
                    .route("", web::get().to(eden_service::comm::apis::get::get_all))
                    .route("/{api}", web::get().to(eden_service::comm::apis::get::get))
                    .route("/{api}", web::patch().to(eden_service::comm::apis::patch::patch))
                    .route("/{api}", web::delete().to(eden_service::comm::apis::delete::delete))
                    .route("/{api}", web::post().to(eden_service::comm::apis::run::run)),
            )
            .service(
                web::scope("/interlays")
                    .wrap(bearer_auth_middleware.clone())
                    .route("", web::get().to(eden_service::comm::interlays::get::get_all))
                    .route("", web::post().to(eden_service::comm::interlays::post::post))
                    .route("/updated", web::get().to(eden_service::comm::interlays::get::get_all_updated))
                    .route("/{interlay}", web::get().to(eden_service::comm::interlays::get::get))
                    .route("/{interlay}", web::delete().to(eden_service::comm::interlays::delete::delete))
                    .route("/{interlay}", web::patch().to(eden_service::comm::interlays::patch::patch))
                    .route("/{interlay}/start", web::post().to(eden_service::comm::interlays::start::start))
                    .route("/{interlay}/stop", web::post().to(eden_service::comm::interlays::stop::stop)),
            )
            .service(
                web::scope("/templates")
                    .wrap(bearer_auth_middleware.clone())
                    .route("", web::post().to(eden_service::comm::templates::post::post))
                    .route("", web::get().to(eden_service::comm::templates::get::get_all))
                    .route("/updated", web::get().to(eden_service::comm::templates::get::get_all_updated))
                    .route("/{template}", web::get().to(eden_service::comm::templates::get::get))
                    .route("/{template}", web::patch().to(eden_service::comm::templates::patch::patch))
                    .route("/{template}", web::delete().to(eden_service::comm::templates::delete::delete))
                    .route("/{template}", web::post().to(eden_service::comm::templates::run::run))
                    .route("/{template}/render", web::post().to(eden_service::comm::templates::render::render)),
            )
            .service(
                web::scope("/workflows")
                    .wrap(bearer_auth_middleware.clone())
                    .route("", web::post().to(eden_service::comm::workflows::post::post))
                    .route("/{workflow}", web::get().to(eden_service::comm::workflows::get::get))
                    .route("/{workflow}", web::patch().to(eden_service::comm::workflows::patch::patch))
                    .route("/{workflow}", web::delete().to(eden_service::comm::workflows::delete::delete)),
            );

        let v1_scope = v1_scope.service(
            web::scope("/analytics")
                .wrap(bearer_auth_middleware.clone())
                .route("/status", web::get().to(eden_service::comm::analytics::status))
                .route("/enable", web::post().to(eden_service::comm::analytics::enable))
                .route("/disable", web::post().to(eden_service::comm::analytics::disable))
                .route("/clickhouse", web::get().to(eden_service::comm::telemetry_analytics::export))
                .route("/clickhouse/{signal}", web::get().to(eden_service::comm::telemetry_analytics::export_signal)),
        );

        let v1_scope = v1_scope.service(
            web::scope("/organizations")
                .wrap(from_fn(eden_service::middleware::org_rate_limit::org_rate_limit))
                .wrap(bearer_auth_middleware.clone())
                .route("", web::get().to(eden_service::comm::organization::get::get))
                .route("", web::patch().to(eden_service::comm::organization::patch::patch))
                .route("/rate-limit", web::get().to(eden_service::comm::organization::rate_limit::get_rate_limit)),
        );

        #[cfg(feature = "llm")]
        let v1_scope = v1_scope.service(
            web::scope("/llm")
                .wrap(from_fn(eden_service::middleware::org_rate_limit::org_rate_limit))
                .wrap(bearer_auth_middleware.clone())
                .route("/chat", web::post().to(eden_service::comm::llm::chat::chat))
                // Gateway request-history log (paginated, filterable). Mirrors the
                // production `/llm` scope route in lib.rs so integration tests
                // exercise the real handler + auth path.
                .route("/gateway/requests", web::get().to(eden_service::comm::llm::requests::gateway_requests)),
        );

        app.service(v1_scope)
    });

    log::debug!("[Server] Attempting to bind to 127.0.0.1:{}...", relay_port);

    let bound_server = _result
        .workers(1)
        .worker_max_blocking_threads(1)
        .bind(("127.0.0.1", relay_port))
        .unwrap_or_else(|e| panic!("Failed to bind to port {}: {}", relay_port, e));

    log::debug!("[Server] Bind successful on 127.0.0.1:{}; starting run()...", relay_port);

    let run_future = bound_server.run();

    log::debug!("[Server] run() future created, awaiting (server is now accepting connections)");

    if let Err(e) = run_future.await {
        eprintln!("Server error: {}", e);
    }

    log::info!("[Server] Server stopped");
}

fn build_single_thread_runtime(name: &str) -> runtime::Runtime {
    runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("can't start {name} runtime: {e}"))
}

fn build_background_runtime(name: &str) -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("can't start {name} runtime: {e}"))
}

pub fn test_server<F, Fut>(tested_function: F, possible_relay_org_token_value: Option<String>)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    // Generate a unique organization ID for this test run to avoid collisions
    let unique_org_id = format!("TestOrg_{}", &uuid::Uuid::new_v4().to_string().replace("-", "")[..8]);

    // Allocate a unique port for this test
    let server_port = find_available_port().unwrap_or(8000);

    let (sender, receiver) = channel();
    // channel to communicate the allocated port back to the test thread
    let (ready_tx, ready_rx) = channel();
    // channel to communicate the TestConfig to the test thread
    let (config_tx, config_rx) = channel();
    let container_thread = std::thread::spawn(move || {
        let rt = build_single_thread_runtime("container");
        let server_rt = build_background_runtime("server");
        let Some((server_handle, _control_plane_db_path_for_cleanup)) = rt.block_on(async {
            let db_containers = match get_shared_containers().await {
                Ok(containers) => {
                    log::info!("Database containers obtained (may be shared from previous test)");
                    containers
                }
                Err(e) => {
                    eprintln!("Failed to initialize database containers: {}", e);
                    return None;
                }
            };

            // Initialize thread-local test configuration with postgres and redis connections
            let pg_conn = db_containers.postgres_conn.clone();
            let redis_conn = db_containers.redis_conn.clone();
            let clickhouse_url_for_config = db_containers.clickhouse_url.clone();
            let control_plane_db_path = db_containers.control_plane_db_path.clone();
            let config = TestConfig {
                server_port,
                org_id: unique_org_id.clone(),
                postgres_conn: pg_conn.clone(),
                redis_conn: redis_conn.clone(),
                clickhouse_url: clickhouse_url_for_config.clone(),
                control_plane_db_path: control_plane_db_path.clone(),
            };
            TestConfig::init(
                server_port,
                unique_org_id.clone(),
                pg_conn,
                redis_conn,
                clickhouse_url_for_config,
                control_plane_db_path,
            );

            // Send the config to the test thread so it can register itself
            let _ = config_tx.send(config);

            // Inform the test thread of the port we've allocated so it can probe it directly
            match ready_tx.send(server_port) {
                Ok(_) => {
                    log::debug!("Signalled test thread with server port {}", server_port)
                }
                Err(e) => eprintln!("Failed to signal test thread port: {}", e),
            }

            let redis_conn = db_containers.redis_conn.clone();
            let pg_conn = db_containers.postgres_conn.clone();
            let clickhouse_url = db_containers.clickhouse_url.clone();
            let control_plane_db_path = db_containers.control_plane_db_path.clone();
            let shared_eden_node_uuid = db_containers.eden_node_uuid.clone();

            // Pass port to the server
            let server_handle = server_rt.handle().spawn(async move {
                start_http_server(
                    &redis_conn,
                    &pg_conn,
                    &clickhouse_url,
                    control_plane_db_path.as_deref(),
                    possible_relay_org_token_value,
                    server_port,
                    shared_eden_node_uuid,
                )
                .await;
            });

            // Wait for the test to complete (with timeout)
            match receiver.recv_timeout(Duration::from_secs(300)) {
                Ok(_) => log::info!("Test completed, shutting down containers"),
                Err(RecvTimeoutError::Timeout) => {
                    eprintln!("Test timeout - containers waited 300 seconds but test never finished");
                }
                Err(RecvTimeoutError::Disconnected) => {
                    eprintln!("Test channel disconnected");
                }
            }

            // Containers are shared across all tests in this process; do not stop them here.
            // They will be cleaned up when the process exits and the Arc is dropped.

            Some((server_handle, db_containers.control_plane_db_path.clone()))
        }) else {
            return;
        };

        server_handle.abort();
        server_rt.shutdown_timeout(Duration::from_secs(5));
        println!("Server shut down");

        #[cfg(embedded_db)]
        if let Some(db_path) = control_plane_db_path_for_cleanup.as_deref() {
            database::test_utils::embedded_db_test_utils::cleanup_test_db_path(db_path);
        }
    });

    // run testing function in a separate runtime and wait until it finishes
    let test_thread = std::thread::spawn(move || {
        let rt = build_single_thread_runtime("testing");
        rt.block_on(async {
            println!("Testing function, waiting for server to start");

            // Use a shorter readiness probe to avoid long (~50s) stalls in CI
            // Most servers are ready within a few seconds due to DB setup
            let max_retries = 200; // ~10 seconds total with 50ms backoff
            let mut retries_left = max_retries;
            let backoff_ms = 50u64;

            // Receive the TestConfig from the container thread.
            // CI can spend several minutes provisioning serialized container-heavy
            // tests late in the suite, so keep this comfortably above the
            // slowest observed startup path.
            let config = match config_rx.recv_timeout(Duration::from_secs(900)) {
                Ok(cfg) => cfg,
                Err(e) => panic!("Failed to receive TestConfig from container thread: {}", e),
            };

            // Register the config in the current thread so get_base_url() works
            TestConfig::init(
                config.server_port,
                config.org_id.clone(),
                config.postgres_conn,
                config.redis_conn,
                config.clickhouse_url,
                config.control_plane_db_path,
            );

            // Wait for the container thread to send the actual allocated port.
            let port = match ready_rx.recv_timeout(Duration::from_secs(900)) {
                Ok(p) => p,
                Err(e) => panic!("Failed to receive server port from container thread: {}", e),
            };

            loop {
                let url = format!("http://localhost:{}/api/v1/auth/login", port);

                // First try a lightweight TCP connect to the server port to detect bind/listen.
                let addr = format!("127.0.0.1:{}", port);
                match addr.parse::<std::net::SocketAddr>() {
                    Ok(sock) => match std::net::TcpStream::connect_timeout(&sock, Duration::from_millis(200)) {
                        Ok(_) => {
                            log::debug!("Server TCP socket listening after {} attempts, starting the test", max_retries - retries_left + 1);
                            break;
                        }
                        Err(e) => {
                            log::debug!("TCP probe connect error to {}: {}", addr, e);
                        }
                    },
                    Err(e) => log::debug!("Invalid socket address {}: {}", addr, e),
                }

                // Fallback: try HTTP endpoint probe (could return 401/500 but server is up)
                match reqwest::Client::new().post(&url).timeout(Duration::from_millis(500)).send().await {
                    Ok(_resp) => {
                        // Server is responding - it's ready
                        log::debug!("Server HTTP probe succeeded after {} attempts, starting the test", max_retries - retries_left + 1);
                        break;
                    }
                    Err(e) => {
                        log::debug!("Server not ready (probe): {}", e);
                    }
                }

                retries_left -= 1;
                if retries_left == 0 {
                    panic!(
                        "Can't connect to the server after {} attempts (~{}ms total wait).",
                        max_retries,
                        max_retries * backoff_ms
                    );
                }

                log::debug!("Retrying to connect to server, {} retries left (waiting {}ms)", retries_left, backoff_ms);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }

            tested_function().await;
            println!("Testing done");
        });
    });

    let mut test_error = false;
    if test_thread.join().is_err() {
        test_error = true;
        eprintln!("Test thread panicked");
    }

    // Close the channel to trigger drop DB containers and stop the server
    let _ = sender.send(());

    if container_thread.join().is_err() {
        eprintln!("Container thread panicked");
        test_error = true;
    }

    if test_error {
        panic!("Error in test");
    }
}
