use super::methods::create::{self};
use crate::db::cache::CacheIdFunctions;
use crate::db::internal_cache::InternalCache;
use crate::sql_file;
use anyhow::Result;
#[cfg(not(embedded_db))]
use clickhouse::Client as ClickhouseClient;
#[cfg(not(embedded_db))]
use deadpool::unmanaged::Pool as ClickhousePool;
use eden_core::auth::Jwt;
use eden_core::error::{EpError, ResultEP};
use eden_core::format::cache_id::{CacheId, OrganizationCacheId};
use eden_core::format::{EdenId, OrganizationId, OrganizationUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::{ctx_with_trace, log_debug};
use ep_core::database::schema::Table;
use ep_core::database::schema::organization::OrganizationSchema;
use function_name::named;
use std::future::Future;
use std::time::Duration;
use uuid::Uuid;

/// Defines the behavior required for internal cache handles.
///
/// The Redis-oriented name is kept to preserve downstream type signatures while
/// the implementation now uses the shared in-process ShardMap cache.
pub trait EdenRedisConnection: Send + Sync + Clone + 'static {
    fn internal_cache(&self) -> InternalCache;
}

/// Compatibility alias for the old internal cache connection type.
pub type RedisConn = InternalCache;

impl EdenRedisConnection for InternalCache {
    fn internal_cache(&self) -> InternalCache {
        self.clone()
    }
}

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
        use crate::db::duckdb_analytics::{DuckDbAnalyticsConfig, DuckDbAnalyticsConnection, DuckDbAnalyticsStore};
        use crate::db::turso::{TursoConnection, TursoPool};

        /// In embedded-db mode, TursoPool serves as the Postgres replacement.
        pub trait EdenPostgresConnection: Send + Sync + 'static {
            fn as_turso_pool(&self) -> &TursoPool;
            fn execute<'a>(
                &'a self,
                query: &'a str,
                params: &'a [&'a (dyn tokio_postgres::types::ToSql + Sync)],
            ) -> impl Future<Output = ResultEP<u64>> + Send + 'a;
            fn batch_execute<'a>(&'a self, query: &'a str) -> impl Future<Output = ResultEP<()>> + Send + 'a;
        }

        pub type PgConn = TursoPool;

        impl EdenPostgresConnection for TursoPool {
            fn as_turso_pool(&self) -> &TursoPool {
                self
            }

            async fn execute<'a>(
                &'a self,
                query: &'a str,
                params: &'a [&'a (dyn tokio_postgres::types::ToSql + Sync)],
            ) -> ResultEP<u64> {
                let conn = self.connect()?;
                conn.execute(query, params).await
            }

            async fn batch_execute<'a>(&'a self, query: &'a str) -> ResultEP<()> {
                let conn = self.connect()?;
                conn.batch_execute(query).await
            }
        }
    } else {
        use bb8::{Pool, PooledConnection};
        use bb8_postgres::PostgresConnectionManager;
        use std::env;
        use tokio_postgres::{Config, NoTls};

        /// Defines the behavior required for Postgres connections for better mocking.
        pub trait EdenPostgresConnection: Send + Sync + 'static {
            fn get_conn<'a>(&'a self) -> impl Future<Output = ResultEP<PooledConnection<'a, PostgresConnectionManager<NoTls>>>> + Send + 'a;
            fn execute<'a>(
                &'a self,
                query: &'a str,
                params: &'a [&'a (dyn tokio_postgres::types::ToSql + Sync)],
            ) -> impl Future<Output = ResultEP<u64>> + Send + 'a;
            fn batch_execute<'a>(&'a self, query: &'a str) -> impl Future<Output = ResultEP<()>> + Send + 'a;
        }

        pub type PgConn = Pool<PostgresConnectionManager<NoTls>>;

        impl EdenPostgresConnection for PgConn {
            async fn get_conn<'a>(&'a self) -> ResultEP<PooledConnection<'a, PostgresConnectionManager<NoTls>>> {
                self.get().await.map_err(|e| EpError::database(format!("Failed to get Postgres connection: {}", e)))
            }

            async fn execute<'a>(
                &'a self,
                query: &'a str,
                params: &'a [&'a (dyn tokio_postgres::types::ToSql + Sync)],
            ) -> ResultEP<u64> {
                let conn = self.get_conn().await?;
                conn.execute(query, params).await.map_err(|e| EpError::database(format!("Failed to execute query: {}", e)))
            }

            async fn batch_execute<'a>(&'a self, query: &'a str) -> ResultEP<()> {
                let conn = self.get_conn().await?;
                conn.batch_execute(query).await.map_err(|e| EpError::database(format!("Failed to batch execute query: {}", e)))
            }
        }

        #[derive(Debug)]
        pub struct DbConfig {
            host: String,
            port: u16,
            username: String,
            password: String,
            database: String,
        }

        impl DbConfig {
            /// Creates DbConfig from environment variables.
            pub fn from_env() -> Self {
                DbConfig {
                    host: env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string()),
                    port: env::var("DB_PORT").unwrap_or_else(|_| "5432".to_string()).parse().unwrap_or_default(),
                    username: env::var("DB_USER").unwrap_or_else(|_| "postgres".to_string()),
                    password: env::var("DB_PASSWORD").expect("DB_PASSWORD must be set"),
                    database: env::var("DB_NAME").unwrap_or_else(|_| "mydb".to_string()),
                }
            }

            /// Converts to tokio_postgres Config.
            pub fn to_config(&self) -> Config {
                let mut config = Config::new();
                config.host(&self.host);
                config.port(self.port);
                config.user(&self.username);
                config.password(&self.password);
                config.dbname(&self.database);
                config
            }
        }
    }
}

/// Defines the behavior required for Clickhouse connections for better mocking
pub trait EdenClickhouseConnection: Send + Sync + 'static {
    fn get_conn<'a>(&'a self) -> impl Future<Output = ResultEP<ClickhousePooledConnection>> + Send + 'a;
}

/// Backend-neutral telemetry analytics operations.
///
/// `ClickhouseConn` is a ClickHouse pool in server builds and a DuckDB store in
/// embedded builds. Keep this dispatch in the database crate so callers do not
/// need to duplicate backend cfg and risk drifting from the selected database
/// feature set.
pub trait EdenTelemetryAnalyticsStorage: Send + Sync + 'static {
    fn ensure_telemetry_tables<'a>(&'a self) -> impl Future<Output = ResultEP<()>> + Send + 'a;

    fn insert_telemetry_rows<'a, T>(&'a self, table: &'a str, rows: &'a [T]) -> impl Future<Output = ResultEP<()>> + Send + 'a
    where
        T: clickhouse::Row + serde::Serialize + Sync + 'a;
}

/// Backend-neutral insert operations for an acquired analytics connection.
pub trait EdenAnalyticsConnectionInsert {
    fn insert_analytics_rows<'a, T>(&'a self, table: &'a str, rows: &'a [T]) -> impl Future<Output = ResultEP<()>> + Send + 'a
    where
        T: clickhouse::Row + serde::Serialize + Sync + 'a;
}

// Real Clickhouse Connection implementation (internal pool)
#[cfg(not(embedded_db))]
pub type ClickhouseConn = ClickhousePool<ClickhouseClient>;
#[cfg(embedded_db)]
pub type ClickhouseConn = DuckDbAnalyticsStore;

#[cfg(embedded_db)]
fn is_valid_turso_hex_key(key: &str) -> bool {
    key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit())
}
#[cfg(not(embedded_db))]
pub type ClickhousePooledConnection = deadpool::unmanaged::Object<ClickhouseClient>;
#[cfg(embedded_db)]
pub type ClickhousePooledConnection = DuckDbAnalyticsConnection;

#[cfg(not(embedded_db))]
impl EdenClickhouseConnection for ClickhouseConn {
    async fn get_conn(&self) -> ResultEP<ClickhousePooledConnection> {
        self.get().await.map_err(|e| EpError::database(format!("Failed to get Clickhouse connection: {}", e)))
    }
}

#[cfg(not(embedded_db))]
impl EdenAnalyticsConnectionInsert for ClickhousePooledConnection {
    #[allow(clippy::manual_async_fn)]
    fn insert_analytics_rows<'a, T>(&'a self, table: &'a str, rows: &'a [T]) -> impl Future<Output = ResultEP<()>> + Send + 'a
    where
        T: clickhouse::Row + serde::Serialize + Sync + 'a,
    {
        async move {
            analytics_schema::insert_batch(self, table, rows)
                .await
                .map_err(|err| EpError::database(format!("failed to insert analytics rows into {table}: {err}")))
        }
    }
}

#[cfg(not(embedded_db))]
impl EdenTelemetryAnalyticsStorage for ClickhouseConn {
    #[allow(clippy::manual_async_fn)]
    fn ensure_telemetry_tables<'a>(&'a self) -> impl Future<Output = ResultEP<()>> + Send + 'a {
        async move {
            let client = self.get_conn().await?;
            analytics_schema::ddl::ensure_telemetry_tables(&client)
                .await
                .map_err(|err| EpError::database(format!("failed to ensure ClickHouse telemetry tables: {err}")))
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn insert_telemetry_rows<'a, T>(&'a self, table: &'a str, rows: &'a [T]) -> impl Future<Output = ResultEP<()>> + Send + 'a
    where
        T: clickhouse::Row + serde::Serialize + Sync + 'a,
    {
        async move {
            let client = self.get_conn().await?;
            client.insert_analytics_rows(table, rows).await
        }
    }
}

#[cfg(embedded_db)]
impl EdenClickhouseConnection for ClickhouseConn {
    async fn get_conn(&self) -> ResultEP<ClickhousePooledConnection> {
        self.get().await
    }
}

#[cfg(embedded_db)]
impl EdenAnalyticsConnectionInsert for ClickhousePooledConnection {
    #[allow(clippy::manual_async_fn)]
    fn insert_analytics_rows<'a, T>(&'a self, table: &'a str, rows: &'a [T]) -> impl Future<Output = ResultEP<()>> + Send + 'a
    where
        T: clickhouse::Row + serde::Serialize + Sync + 'a,
    {
        async move { self.insert_rows(table, rows).await }
    }
}

#[cfg(embedded_db)]
impl EdenTelemetryAnalyticsStorage for ClickhouseConn {
    #[allow(clippy::manual_async_fn)]
    fn ensure_telemetry_tables<'a>(&'a self) -> impl Future<Output = ResultEP<()>> + Send + 'a {
        async move { self.ensure_schema().await }
    }

    #[allow(clippy::manual_async_fn)]
    fn insert_telemetry_rows<'a, T>(&'a self, table: &'a str, rows: &'a [T]) -> impl Future<Output = ResultEP<()>> + Send + 'a
    where
        T: clickhouse::Row + serde::Serialize + Sync + 'a,
    {
        async move {
            let client = self.get_conn().await?;
            client.insert_analytics_rows(table, rows).await
        }
    }
}

#[cfg(not(embedded_db))]
pub type AnalyticsDbConfig = ClickhouseDbConfig;
#[cfg(embedded_db)]
pub type AnalyticsDbConfig = DuckDbAnalyticsConfig;

#[cfg(not(embedded_db))]
const CLICKHOUSE_POOL_SIZE_ENV: &str = "CLICKHOUSE_POOL_SIZE";
pub const DEFAULT_CLICKHOUSE_POOL_SIZE: usize = 8;

macro_rules! init_schema_sql {
    ($conn:expr, $sql:expr, $context:expr) => {
        $conn.batch_execute($sql).await.map_err(|e| EpError::init(format!("{}: {e}", $context)))?;
    };
}

#[cfg(not(embedded_db))]
fn normalize_optional(value: Option<String>) -> Option<String> {
    value.map(|raw| raw.trim().to_string()).filter(|trimmed| !trimmed.is_empty())
}

/// Internal Clickhouse configuration for DatabaseManager.
#[cfg(not(embedded_db))]
#[derive(Debug, Clone)]
pub struct ClickhouseDbConfig {
    url: String,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    pool_size: usize,
}

#[cfg(not(embedded_db))]
impl ClickhouseDbConfig {
    pub fn new(
        url: impl Into<String>,
        user: Option<String>,
        password: Option<String>,
        database: Option<String>,
        pool_size: usize,
    ) -> ResultEP<Self> {
        let raw_url = url.into();
        let trimmed_url = raw_url.trim();
        if trimmed_url.is_empty() {
            return Err(EpError::database("Clickhouse URL must be set"));
        }
        if pool_size == 0 {
            return Err(EpError::database(format!("{} must be greater than zero", CLICKHOUSE_POOL_SIZE_ENV)));
        }

        Ok(Self {
            url: trimmed_url.to_string(),
            user: normalize_optional(user),
            password: normalize_optional(password),
            database: normalize_optional(database),
            pool_size,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub fn pool_size(&self) -> usize {
        self.pool_size
    }
}

/// Type-safe cache TTL to prevent unit confusion (seconds vs milliseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheTtl(u64);

impl CacheTtl {
    /// Create from seconds.
    pub fn from_secs(secs: u64) -> Self {
        Self(secs)
    }
    /// Return the TTL as raw seconds.
    pub fn as_secs(&self) -> u64 {
        self.0
    }
    /// Convert to `std::time::Duration`.
    pub fn as_duration(&self) -> Duration {
        Duration::from_secs(self.0)
    }
}

/// Shared in-process cache -> Postgres (+ internal Clickhouse)
pub struct DatabaseManager<R, P, C> {
    cache_pool: R,
    postgres_pool: P,
    clickhouse_pool: C,
    cache_ttl: CacheTtl,
    pub(crate) jwt: Option<Jwt>,
    /// Org-key provider for ELS config encryption. `None` disables encryption
    /// (embedded-db, tests without encryption).
    org_key_provider: Option<std::sync::Arc<dyn super::encryption::OrgKeyProvider>>,
    #[cfg_attr(embedded_db, allow(dead_code))]
    /// Original PG connection URL, used by pg_dump for backups.
    pub(crate) pg_url: String,
}

/// Low-level cache backend access for internal database operations.
///
/// The trait name is retained for API compatibility; implementations return
/// the shared in-process ShardMap cache handle.
pub trait ShardCache {
    fn internal_cache(&self) -> InternalCache;
    fn cache_connection(&self) -> impl Future<Output = ResultEP<InternalCache>> + Send;
    fn rbac_connection(&self) -> impl Future<Output = ResultEP<InternalCache>> + Send;
}

impl<R, P, C> ShardCache for DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: Sync,
    C: EdenClickhouseConnection + Sync,
{
    fn internal_cache(&self) -> InternalCache {
        self.cache_pool.internal_cache()
    }

    async fn cache_connection(&self) -> ResultEP<InternalCache> {
        Ok(self.cache_pool.internal_cache())
    }

    async fn rbac_connection(&self) -> ResultEP<InternalCache> {
        Ok(self.cache_pool.internal_cache())
    }
}

cfg_if::cfg_if! {
    if #[cfg(not(embedded_db))] {
        const DEFAULT_POSTGRES_POOL_SIZE: u32 = 32;

        pub async fn create_redis_connection(_connection_string: &str, _database: usize) -> ResultEP<RedisConn> {
            Ok(InternalCache::new())
        }

        pub async fn create_postgres_connection(connection_string: &str) -> ResultEP<PgConn> {
            let manager = PostgresConnectionManager::new_from_stringlike(connection_string, NoTls)
                .map_err(|e| EpError::database(format!("Failed to create Postgres manager: {}", e)))?;

            Pool::builder()
                .max_size(DEFAULT_POSTGRES_POOL_SIZE)
                .min_idle(None)
                .max_lifetime(Some(Duration::from_secs(60 * 30)))
                .idle_timeout(Some(Duration::from_secs(60 * 5)))
                .connection_timeout(Duration::from_secs(120))
                .build(manager)
                .await
                .map_err(|e| EpError::database(format!("Failed to create Postgres pool: {}", e)))
        }
    }
}

/// Creates a Clickhouse connection pool for internal usage.
#[cfg(not(embedded_db))]
pub fn create_clickhouse_connection(config: &ClickhouseDbConfig) -> ResultEP<ClickhouseConn> {
    if config.pool_size() == 0 {
        return Err(EpError::database(format!("{} must be greater than zero", CLICKHOUSE_POOL_SIZE_ENV)));
    }

    let mut clients = Vec::with_capacity(config.pool_size());
    for _ in 0..config.pool_size() {
        let mut client = ClickhouseClient::default().with_url(config.url().to_string());

        if let Some(database) = config.database() {
            client = client.with_database(database.to_string());
        }

        if let Some(user) = config.user() {
            client = client.with_user(user.to_string());
        }

        if let Some(password) = config.password() {
            client = client.with_password(password.to_string());
        }

        client = client.with_header("X-ClickHouse-Format", "json");
        client = client.with_header("Accept", "application/json");

        clients.push(client);
    }

    Ok(ClickhousePool::from(clients))
}

/// Creates the embedded DuckDB analytics store for internal usage.
#[cfg(embedded_db)]
pub async fn create_clickhouse_connection(config: &DuckDbAnalyticsConfig) -> ResultEP<ClickhouseConn> {
    DuckDbAnalyticsStore::new(config.clone()).await
}

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
        async fn initialize_template_schema_updates(_pool: &impl EdenPostgresConnection) -> Result<(), EpError> {
            Ok(())
        }

        async fn initialize_llm_skill_schema_updates(_pool: &impl EdenPostgresConnection) -> Result<(), EpError> {
            Ok(())
        }
    } else {
        async fn initialize_template_schema_updates(pool: &impl EdenPostgresConnection) -> Result<(), EpError> {
            init_schema_sql!(
                pool,
                sql_file!("update", "add_template_llm_recommendation_column"),
                "Failed to add llm_recommendation column to templates table"
            );
            Ok(())
        }

        async fn initialize_llm_skill_schema_updates(pool: &impl EdenPostgresConnection) -> Result<(), EpError> {
            init_schema_sql!(
                pool,
                sql_file!("update", "add_llm_skills_source_columns"),
                "Failed to add source columns to llm_skills table"
            );
            init_schema_sql!(
                pool,
                sql_file!("update", "add_llm_skills_tier_columns"),
                "Failed to add tier columns to llm_skills table"
            );
            // Tenant-scoping column for llm_skills. Idempotent under either
            // a green-field install (column ships in create/llm_skills.sql)
            // or an upgrade from the legacy `name UNIQUE` schema.
            init_schema_sql!(
                pool,
                sql_file!("update", "add_llm_skills_organization_uuid_column"),
                "Failed to add organization_uuid column and per-scope uniqueness indexes to llm_skills table"
            );
            Ok(())
        }
    }
}

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Create a new DatabaseManager with provided cache, Postgres, and Clickhouse connections.
    pub fn new_with_connections(
        cache_pool: R,
        _redis_rbac: R,
        postgres_pool: P,
        clickhouse_pool: C,
        cache_ttl: CacheTtl,
        jwt: Option<Jwt>,
    ) -> Self {
        Self {
            cache_pool,
            postgres_pool,
            clickhouse_pool,
            cache_ttl,
            jwt,
            org_key_provider: None,
            pg_url: String::new(),
        }
    }

    /// Set the org-key provider for ELS config encryption.
    pub fn with_org_key_provider(mut self, provider: std::sync::Arc<dyn super::encryption::OrgKeyProvider>) -> Self {
        self.org_key_provider = Some(provider);
        self
    }

    /// Get the org-key provider, if configured.
    pub fn org_key_provider(&self) -> Option<&dyn super::encryption::OrgKeyProvider> {
        self.org_key_provider.as_deref()
    }

    cfg_if::cfg_if! {
        if #[cfg(embedded_db)] {
        } else {
            pub fn pg_url(&self) -> &str {
                &self.pg_url
            }
        }
    }

    /// Get the process-local internal cache handle used by RBAC and ELS.
    ///
    /// Used by the PG proxy to look up per-user ELS session variables.
    pub fn rbac_redis_pool(&self) -> InternalCache {
        self.cache_pool.internal_cache()
    }

    /// Return the original cache connection handle retained for API compatibility.
    pub fn cache_pool(&self) -> &R {
        &self.cache_pool
    }

    cfg_if::cfg_if! {
        if #[cfg(embedded_db)] {
        } else {
            /// Create a new DatabaseManager with connection strings and Clickhouse config.
            #[named]
            pub async fn new(
                _redis_connections: &str,
                pg_connection: &str,
                clickhouse_config: AnalyticsDbConfig,
                cache_ttl: CacheTtl,
                jwt: Option<Jwt>,
            ) -> Result<DatabaseManager<RedisConn, PgConn, ClickhouseConn>, EpError> {
                let _ctx = ctx_with_trace!().with_feature("database");

                log_debug!(
                    _ctx.clone(),
                    "Configuring internal ShardMap cache",
                    audience = eden_logger_internal::LogAudience::Internal
                );
                let internal_cache = InternalCache::new();

                log_debug!(
                    _ctx.clone(),
                    "Connecting to PostgreSQL",
                    audience = eden_logger_internal::LogAudience::Internal,
                    pg_connection = pg_connection
                );
                let postgres_pool = create_postgres_connection(pg_connection).await?;

                Self::initialize_database(&postgres_pool)
                    .await
                    .map_err(|e| EpError::init(format!("Failed to initialize postgres: {e}")))?;

                log_debug!(
                    _ctx.clone(),
                    "Configuring internal Clickhouse",
                    audience = eden_logger_internal::LogAudience::Internal,
                    clickhouse_url = clickhouse_config.url()
                );
                let clickhouse_pool = create_clickhouse_connection(&clickhouse_config)?;

                let mut db = DatabaseManager::new_with_connections(
                    internal_cache.clone(),
                    internal_cache,
                    postgres_pool,
                    clickhouse_pool,
                    cache_ttl,
                    jwt,
                )
                .with_org_key_provider(std::sync::Arc::new(super::encryption::EnvKeyProvider));
                db.pg_url = pg_connection.to_string();
                Ok(db)
            }
        }
    }

    /// Create a new DatabaseManager backed by Turso and in-memory cache.
    ///
    /// Used with the `embedded-db` feature for running Eden without external
    /// PostgreSQL services.
    #[cfg(embedded_db)]
    #[named]
    pub async fn new_local(
        turso_path: &str,
        analytics_config: AnalyticsDbConfig,
        cache_ttl: CacheTtl,
        jwt: Option<Jwt>,
        encryption_key: Option<String>,
    ) -> Result<DatabaseManager<RedisConn, TursoPool, ClickhouseConn>, EpError> {
        let _ctx = ctx_with_trace!().with_feature("database");
        let is_file_backed = turso_path != ":memory:";

        log_debug!(
            _ctx.clone(),
            "Initializing Turso database",
            audience = eden_logger_internal::LogAudience::Internal,
            turso_path = turso_path,
            encrypted = encryption_key.is_some().to_string(),
            mode = if is_file_backed { "file" } else { "memory" }
        );

        // Auto-create parent directory for file-backed databases.
        if is_file_backed {
            let path = std::path::Path::new(turso_path);
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| EpError::init(format!("Failed to create database directory {}: {e}", parent.display())))?;
            }
        }

        if is_file_backed && encryption_key.is_none() {
            return Err(EpError::init(
                "EDEN_DB_ENCRYPTION_KEY must be set for file-backed embedded-db databases".to_string(),
            ));
        }

        let effective_path = if is_file_backed {
            turso_path.to_string()
        } else {
            "file::memory:?cache=shared".to_string()
        };

        let mut builder = turso::Builder::new_local(&effective_path);

        // Turso local encryption is currently gated behind the experimental flag.
        if is_file_backed {
            builder = builder.experimental_encryption(true);
            if let Some(key) = encryption_key.as_deref() {
                if !is_valid_turso_hex_key(key) {
                    return Err(EpError::init(
                        "EDEN_DB_ENCRYPTION_KEY must be a 64-character hexadecimal key for embedded-db databases".to_string(),
                    ));
                }
                builder = builder.with_encryption(turso::EncryptionOpts { cipher: "aes256gcm".to_string(), hexkey: key.to_string() });
            }
        }

        let db = builder.build().await.map_err(|e| EpError::init(format!("Failed to build Turso database: {e}")))?;

        let turso_pool = TursoPool::new(db);
        let internal_cache = InternalCache::new();
        let clickhouse_pool = create_clickhouse_connection(&analytics_config).await?;

        // Use a single connection for all DDL (in-memory DBs are per-connection)
        let init_conn = turso_pool.connect()?;

        // Enable foreign key enforcement for SQLite
        init_conn
            .execute("PRAGMA foreign_keys = ON", &[])
            .await
            .map_err(|e| EpError::init(format!("Failed to enable foreign keys: {e}")))?;

        // WAL mode + synchronous=NORMAL for crash-safe durability on file-backed databases.
        // WAL allows concurrent readers during writes and survives application crashes.
        // NORMAL sync: WAL is fsynced at checkpoint, not every commit — good perf/safety tradeoff.
        if is_file_backed {
            init_conn
                .query("PRAGMA journal_mode = WAL", &[])
                .await
                .map_err(|e| EpError::init(format!("Failed to enable WAL mode: {e}")))?;
            init_conn
                .query("PRAGMA synchronous = NORMAL", &[])
                .await
                .map_err(|e| EpError::init(format!("Failed to set synchronous mode: {e}")))?;
        }

        Self::initialize_database_local(&init_conn)
            .await
            .map_err(|e| EpError::init(format!("Failed to initialize Turso database: {e}")))?;

        log_debug!(_ctx, "Turso database initialized", audience = eden_logger_internal::LogAudience::Internal);

        Ok(
            DatabaseManager::new_with_connections(internal_cache.clone(), internal_cache, turso_pool, clickhouse_pool, cache_ttl, jwt)
                .with_org_key_provider(std::sync::Arc::new(super::encryption::EnvKeyProvider)),
        )
    }

    /// Initialize the database tables
    pub async fn initialize_database(pool: &impl EdenPostgresConnection) -> Result<(), EpError> {
        // Database bootstrap runs all static DDL through batch_execute so both
        // single-statement and multi-statement schema SQL stay safe.
        init_schema_sql!(pool, create::CREATE_PGCRYPTO_EXTENSION, "Failed to enable pgcrypto extension");
        init_schema_sql!(pool, create::CREATE_ENDPOINTS, "Failed to create endpoints table");
        init_schema_sql!(pool, create::CREATE_TEMPLATES, "Failed to create templates table");
        initialize_template_schema_updates(pool).await?;

        init_schema_sql!(pool, create::CREATE_APIS, "Failed to create apis table");

        init_schema_sql!(pool, create::CREATE_JWT_BLACKLIST, "Failed to create jwt_blacklist table");
        init_schema_sql!(pool, create::CREATE_AUTHS, "Failed to create auths table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATIONS, "Failed to create organizations table");
        #[cfg(not(embedded_db))]
        init_schema_sql!(
            pool,
            sql_file!("update", "add_organization_rate_limit_settings_column"),
            "Failed to add rate_limit_settings column to organizations table"
        );
        init_schema_sql!(pool, create::CREATE_USERS, "Failed to create users table");
        init_schema_sql!(pool, create::CREATE_ANALYTICS_DASHBOARD_PREFS, "Failed to create analytics_dashboard_prefs table");
        init_schema_sql!(pool, create::CREATE_ROBOTS, "Failed to create robots table");
        init_schema_sql!(pool, create::CREATE_WORKFLOWS, "Failed to create workflows table");
        init_schema_sql!(pool, create::CREATE_INTERLAYS, "Failed to create interlays table");
        init_schema_sql!(pool, create::CREATE_RBAC_TYPES, "Failed to create RBAC shared types");
        init_schema_sql!(pool, create::CREATE_RBAC_CONTROL, "Failed to create rbac_control table");
        init_schema_sql!(
            pool,
            create::CREATE_RBAC_CONTROL_ROW_TOMBSTONES,
            "Failed to create rbac_control_row_tombstones table"
        );
        init_schema_sql!(
            pool,
            create::CREATE_RBAC_CONTROL_ENTITY_TOMBSTONES,
            "Failed to create rbac_control_entity_tombstones table"
        );
        init_schema_sql!(
            pool,
            create::CREATE_RBAC_CONTROL_SUBJECT_TOMBSTONES,
            "Failed to create rbac_control_subject_tombstones table"
        );
        init_schema_sql!(pool, create::CREATE_RBAC_DATA, "Failed to create rbac_data table");
        init_schema_sql!(pool, create::CREATE_RBAC_DATA_ROW_TOMBSTONES, "Failed to create rbac_data_row_tombstones table");
        init_schema_sql!(
            pool,
            create::CREATE_RBAC_DATA_ENTITY_TOMBSTONES,
            "Failed to create rbac_data_entity_tombstones table"
        );
        init_schema_sql!(
            pool,
            create::CREATE_RBAC_DATA_SUBJECT_TOMBSTONES,
            "Failed to create rbac_data_subject_tombstones table"
        );
        init_schema_sql!(pool, create::CREATE_LLM_AGENTS, "Failed to create llm_agents table");
        init_schema_sql!(pool, create::CREATE_LLM_AGENT_VERSIONS, "Failed to create llm_agent_versions table");
        init_schema_sql!(pool, create::CREATE_LLM_AGENT_RUNS, "Failed to create llm_agent_runs table");
        init_schema_sql!(pool, create::CREATE_AGENT_METRICS_HOURLY, "Failed to create agent_metrics_hourly table");
        init_schema_sql!(pool, create::CREATE_LLM_NOTIFICATIONS, "Failed to create llm_notifications table");
        init_schema_sql!(pool, create::CREATE_USER_NOTIFICATIONS, "Failed to create user_notifications table");
        init_schema_sql!(pool, create::CREATE_WORKSPACE_VIEWS, "Failed to create workspace_views table");
        init_schema_sql!(pool, create::CREATE_EXECUTION_RUNS, "Failed to create execution_runs table");
        init_schema_sql!(pool, create::CREATE_EVIDENCE_RECORDS, "Failed to create evidence_records table");
        init_schema_sql!(pool, create::CREATE_RUN_EVENTS, "Failed to create run_events table");
        init_schema_sql!(pool, create::CREATE_TRIGGER_SOURCES, "Failed to create trigger_sources table");
        init_schema_sql!(pool, create::CREATE_TRIGGER_EVENTS, "Failed to create trigger_events table");
        init_schema_sql!(pool, create::CREATE_AGENT_TRIGGER_RULES, "Failed to create agent_trigger_rules table");
        init_schema_sql!(
            pool,
            sql_file!("update", "add_execution_runs_idempotency_key_column"),
            "Failed to add idempotency_key column to execution_runs table"
        );
        init_schema_sql!(
            pool,
            sql_file!("update", "execution_runs_add_cost"),
            "Failed to add cost columns to execution_runs table"
        );
        init_schema_sql!(pool, create::CREATE_LLM_SYSTEM_PROMPTS, "Failed to create llm_system_prompts table");
        init_schema_sql!(pool, create::CREATE_LLM_CREDENTIALS, "Failed to create llm_credentials table");
        init_schema_sql!(pool, create::CREATE_LLM_GATEWAY_API_KEYS, "Failed to create llm_gateway_api_keys table");
        init_schema_sql!(pool, create::CREATE_LLM_ORG_PII_DICTIONARY, "Failed to create llm_org_pii_dictionary table");
        init_schema_sql!(pool, create::CREATE_LLM_GATEWAY_RESPONSE_CACHE, "Failed to create llm_gateway_response_cache table");
        init_schema_sql!(pool, create::CREATE_LLM_GATEWAY_ROUTE_ROLLUPS, "Failed to create llm_gateway_route_rollups table");
        init_schema_sql!(pool, create::CREATE_LLM_GATEWAY_USAGE_ROLLUPS, "Failed to create llm_gateway_usage_rollups table");
        init_schema_sql!(pool, create::CREATE_LLM_USER_TOOLS_ENDPOINTS, "Failed to create llm_user_tools_endpoints table");
        init_schema_sql!(pool, create::CREATE_EDEN_NODES, "Failed to create edenNodes table");
        init_schema_sql!(pool, create::CREATE_EDEN_NODE_ENDPOINTS, "Failed to create eden_node_endpoints table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_USERS, "Failed to create organization_users table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_ROBOTS, "Failed to create organization_robots table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_ENDPOINTS, "Failed to create organization_endpoints table");
        init_schema_sql!(pool, create::CREATE_ENDPOINT_GROUPS, "Failed to create endpoint_groups table");
        init_schema_sql!(pool, create::CREATE_ENDPOINT_GROUP_MEMBERS, "Failed to create endpoint_group_members table");
        init_schema_sql!(
            pool,
            create::CREATE_ORGANIZATION_ENDPOINT_GROUPS,
            "Failed to create organization_endpoint_groups table"
        );
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_ADMINS, "Failed to create organization_admins table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_EDEN_NODES, "Failed to create organization_eden_nodes table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_TEMPLATES, "Failed to create organization_templates table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_APIS, "Failed to create organization_apis table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_WORKFLOWS, "Failed to create organization_workflows table");
        init_schema_sql!(pool, create::CREATE_WORKFLOW_TEMPLATES, "Failed to create workflow_templates table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_INTERLAYS, "Failed to create organization_interlays table");
        init_schema_sql!(pool, create::CREATE_SNAPSHOTS, "Failed to create snapshots table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_SNAPSHOTS, "Failed to create organization_snapshots table");
        init_schema_sql!(pool, create::CREATE_PIPELINES, "Failed to create pipelines table");
        init_schema_sql!(pool, create::CREATE_ORGANIZATION_PIPELINES, "Failed to create organization_pipelines table");
        init_schema_sql!(pool, create::CREATE_LLM_AGENTS_INDEX, "Failed to create llm_agents org_status index");
        init_schema_sql!(pool, create::CREATE_LLM_AGENTS_NEXT_RUN_INDEX, "Failed to create llm_agents next_run index");
        init_schema_sql!(pool, create::CREATE_LLM_AGENT_VERSIONS_INDEX, "Failed to create llm_agent_versions index");
        init_schema_sql!(pool, create::CREATE_LLM_AGENT_RUNS_INDEX, "Failed to create llm_agent_runs agent index");
        init_schema_sql!(pool, create::CREATE_LLM_AGENT_RUNS_STATUS_INDEX, "Failed to create llm_agent_runs status index");
        init_schema_sql!(pool, create::CREATE_LLM_NOTIFICATIONS_INDEX, "Failed to create llm_notifications indexes");
        init_schema_sql!(pool, create::CREATE_WORKSPACE_VIEWS_INDEX, "Failed to create workspace_views indexes");
        init_schema_sql!(pool, create::CREATE_EXECUTION_RUNS_INDEX, "Failed to create execution_runs indexes");
        init_schema_sql!(pool, create::CREATE_EVIDENCE_RECORDS_INDEX, "Failed to create evidence_records indexes");
        init_schema_sql!(pool, create::CREATE_RUN_EVENTS_INDEX, "Failed to create run_events indexes");
        init_schema_sql!(pool, create::CREATE_TRIGGER_SOURCES_INDEX, "Failed to create trigger_sources indexes");
        init_schema_sql!(pool, create::CREATE_TRIGGER_EVENTS_INDEX, "Failed to create trigger_events indexes");
        init_schema_sql!(
            pool,
            create::CREATE_TRIGGER_EVENTS_IDEMPOTENCY_INDEX,
            "Failed to create trigger_events idempotency index"
        );
        init_schema_sql!(pool, create::CREATE_AGENT_TRIGGER_RULES_INDEX, "Failed to create agent_trigger_rules indexes");
        init_schema_sql!(pool, create::CREATE_LLM_CREDENTIALS_INDEX, "Failed to create llm_credentials indexes");
        init_schema_sql!(pool, create::CREATE_LLM_GATEWAY_API_KEYS_INDEX, "Failed to create llm_gateway_api_keys indexes");
        init_schema_sql!(
            pool,
            create::CREATE_LLM_GATEWAY_RESPONSE_CACHE_INDEX,
            "Failed to create llm_gateway_response_cache indexes"
        );
        init_schema_sql!(
            pool,
            create::CREATE_LLM_GATEWAY_ROUTE_ROLLUPS_INDEX,
            "Failed to create llm_gateway_route_rollups indexes"
        );
        init_schema_sql!(
            pool,
            create::CREATE_LLM_GATEWAY_USAGE_ROLLUPS_INDEX,
            "Failed to create llm_gateway_usage_rollups indexes"
        );
        init_schema_sql!(
            pool,
            create::CREATE_LLM_USER_TOOLS_ENDPOINTS_INDEX,
            "Failed to create llm_user_tools_endpoints indexes"
        );
        init_schema_sql!(
            pool,
            create::CREATE_LLM_CREDENTIALS_UNIQUE_LABEL_INDEX,
            "Failed to create llm_credentials unique label index"
        );
        init_schema_sql!(pool, create::CREATE_LLM_SKILLS, "Failed to create llm_skills table");
        initialize_llm_skill_schema_updates(pool).await?;

        init_schema_sql!(pool, create::CREATE_ELS_POLICIES, "Failed to create els_policies table");
        init_schema_sql!(pool, create::CREATE_ELS_POLICY_ASSIGNMENTS, "Failed to create els_policy_assignments table");
        init_schema_sql!(pool, create::CREATE_ELS_POLICY_VERSIONS, "Failed to create els_policy_versions table");
        init_schema_sql!(pool, create::CREATE_ELS_POLICY_POINTERS, "Failed to create els_policy_pointers table");
        init_schema_sql!(pool, create::CREATE_ORG_KEY_REFS, "Failed to create org_key_refs table");
        init_schema_sql!(pool, create::CREATE_ENCRYPTION_KEYS, "Failed to create encryption_keys table");

        Ok(())
    }

    /// Initialize only the AI-relevant tables for embedded-db mode.
    ///
    /// Skips: migrations, interlays, eden_nodes, snapshots, pipelines.
    /// Keeps: organizations, users, robots, auths, endpoints, endpoint_groups,
    ///        templates, apis, workflows, rbac, and all llm_* tables.
    #[cfg(embedded_db)]
    pub async fn initialize_database_local(conn: &TursoConnection) -> Result<(), EpError> {
        init_schema_sql!(conn, create::CREATE_ORGANIZATIONS, "organizations");
        init_schema_sql!(conn, create::CREATE_USERS, "users");
        init_schema_sql!(conn, create::CREATE_ANALYTICS_DASHBOARD_PREFS, "analytics_dashboard_prefs");
        init_schema_sql!(conn, create::CREATE_ROBOTS, "robots");
        init_schema_sql!(conn, create::CREATE_AUTHS, "auths");
        init_schema_sql!(conn, create::CREATE_RBAC_TYPES, "rbac_types");
        init_schema_sql!(conn, create::CREATE_RBAC_CONTROL, "rbac_control");
        init_schema_sql!(conn, create::CREATE_RBAC_CONTROL_ROW_TOMBSTONES, "rbac_control_row_tombstones");
        init_schema_sql!(conn, create::CREATE_RBAC_CONTROL_ENTITY_TOMBSTONES, "rbac_control_entity_tombstones");
        init_schema_sql!(conn, create::CREATE_RBAC_CONTROL_SUBJECT_TOMBSTONES, "rbac_control_subject_tombstones");
        init_schema_sql!(conn, create::CREATE_RBAC_DATA, "rbac_data");
        init_schema_sql!(conn, create::CREATE_RBAC_DATA_ROW_TOMBSTONES, "rbac_data_row_tombstones");
        init_schema_sql!(conn, create::CREATE_RBAC_DATA_ENTITY_TOMBSTONES, "rbac_data_entity_tombstones");
        init_schema_sql!(conn, create::CREATE_RBAC_DATA_SUBJECT_TOMBSTONES, "rbac_data_subject_tombstones");
        init_schema_sql!(conn, create::CREATE_EDEN_NODES, "eden_nodes");
        init_schema_sql!(conn, create::CREATE_EDEN_NODE_ENDPOINTS, "eden_node_endpoints");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_EDEN_NODES, "organization_eden_nodes");
        init_schema_sql!(conn, create::CREATE_ENDPOINTS, "endpoints");
        init_schema_sql!(conn, create::CREATE_ENDPOINT_GROUPS, "endpoint_groups");
        init_schema_sql!(conn, create::CREATE_ENDPOINT_GROUP_MEMBERS, "endpoint_group_members");
        init_schema_sql!(conn, create::CREATE_TEMPLATES, "templates");
        init_schema_sql!(conn, create::CREATE_APIS, "apis");
        init_schema_sql!(conn, create::CREATE_WORKFLOWS, "workflows");
        init_schema_sql!(conn, create::CREATE_INTERLAYS, "interlays");
        init_schema_sql!(conn, create::CREATE_JWT_BLACKLIST, "jwt_blacklist");
        init_schema_sql!(conn, create::CREATE_SNAPSHOTS, "snapshots");
        init_schema_sql!(conn, create::CREATE_PIPELINES, "pipelines");
        init_schema_sql!(conn, create::CREATE_WORKFLOW_TEMPLATES, "workflow_templates");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_ADMINS, "organization_admins");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_USERS, "organization_users");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_ROBOTS, "organization_robots");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_ENDPOINTS, "organization_endpoints");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_ENDPOINT_GROUPS, "organization_endpoint_groups");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_TEMPLATES, "organization_templates");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_APIS, "organization_apis");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_WORKFLOWS, "organization_workflows");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_INTERLAYS, "organization_interlays");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_SNAPSHOTS, "organization_snapshots");
        init_schema_sql!(conn, create::CREATE_ORGANIZATION_PIPELINES, "organization_pipelines");
        init_schema_sql!(conn, create::CREATE_LLM_AGENTS, "llm_agents");
        init_schema_sql!(conn, create::CREATE_LLM_AGENT_VERSIONS, "llm_agent_versions");
        init_schema_sql!(conn, create::CREATE_LLM_AGENT_RUNS, "llm_agent_runs");
        init_schema_sql!(conn, create::CREATE_AGENT_METRICS_HOURLY, "agent_metrics_hourly");
        init_schema_sql!(conn, create::CREATE_LLM_NOTIFICATIONS, "llm_notifications");
        init_schema_sql!(conn, create::CREATE_WORKSPACE_VIEWS, "workspace_views");
        init_schema_sql!(conn, create::CREATE_EXECUTION_RUNS, "execution_runs");
        init_schema_sql!(conn, create::CREATE_EVIDENCE_RECORDS, "evidence_records");
        init_schema_sql!(conn, create::CREATE_RUN_EVENTS, "run_events");
        init_schema_sql!(conn, create::CREATE_TRIGGER_SOURCES, "trigger_sources");
        init_schema_sql!(conn, create::CREATE_TRIGGER_EVENTS, "trigger_events");
        init_schema_sql!(conn, create::CREATE_AGENT_TRIGGER_RULES, "agent_trigger_rules");
        init_schema_sql!(conn, sql_file!("update", "add_execution_runs_idempotency_key_column"), "execution_runs_idempotency");
        init_schema_sql!(conn, sql_file!("update", "execution_runs_add_cost"), "execution_runs_cost_columns");
        init_schema_sql!(conn, create::CREATE_LLM_SYSTEM_PROMPTS, "llm_system_prompts");
        init_schema_sql!(conn, create::CREATE_LLM_CREDENTIALS, "llm_credentials");
        init_schema_sql!(conn, create::CREATE_LLM_GATEWAY_API_KEYS, "llm_gateway_api_keys");
        init_schema_sql!(conn, create::CREATE_LLM_ORG_PII_DICTIONARY, "llm_org_pii_dictionary");
        init_schema_sql!(conn, create::CREATE_LLM_GATEWAY_RESPONSE_CACHE, "llm_gateway_response_cache");
        init_schema_sql!(conn, create::CREATE_LLM_GATEWAY_ROUTE_ROLLUPS, "llm_gateway_route_rollups");
        init_schema_sql!(conn, create::CREATE_LLM_GATEWAY_USAGE_ROLLUPS, "llm_gateway_usage_rollups");
        init_schema_sql!(conn, create::CREATE_LLM_USER_TOOLS_ENDPOINTS, "llm_user_tools_endpoints");
        init_schema_sql!(conn, create::CREATE_LLM_SKILLS, "llm_skills");
        init_schema_sql!(conn, create::CREATE_LLM_AGENTS_INDEX, "llm_agents_index");
        init_schema_sql!(conn, create::CREATE_LLM_AGENTS_NEXT_RUN_INDEX, "llm_agents_next_run_index");
        init_schema_sql!(conn, create::CREATE_LLM_AGENT_VERSIONS_INDEX, "llm_agent_versions_index");
        init_schema_sql!(conn, create::CREATE_LLM_AGENT_RUNS_INDEX, "llm_agent_runs_index");
        init_schema_sql!(conn, create::CREATE_WORKSPACE_VIEWS_INDEX, "workspace_views_index");
        init_schema_sql!(conn, create::CREATE_LLM_AGENT_RUNS_STATUS_INDEX, "llm_agent_runs_status_index");
        init_schema_sql!(conn, create::CREATE_LLM_NOTIFICATIONS_INDEX, "llm_notifications_index");
        init_schema_sql!(conn, create::CREATE_EXECUTION_RUNS_INDEX, "execution_runs_index");
        init_schema_sql!(conn, create::CREATE_EVIDENCE_RECORDS_INDEX, "evidence_records_index");
        init_schema_sql!(conn, create::CREATE_RUN_EVENTS_INDEX, "run_events_index");
        init_schema_sql!(conn, create::CREATE_TRIGGER_SOURCES_INDEX, "trigger_sources_index");
        init_schema_sql!(conn, create::CREATE_TRIGGER_EVENTS_INDEX, "trigger_events_index");
        init_schema_sql!(conn, create::CREATE_TRIGGER_EVENTS_IDEMPOTENCY_INDEX, "trigger_events_idempotency_index");
        init_schema_sql!(conn, create::CREATE_AGENT_TRIGGER_RULES_INDEX, "agent_trigger_rules_index");
        init_schema_sql!(conn, create::CREATE_LLM_CREDENTIALS_INDEX, "llm_credentials_index");
        init_schema_sql!(conn, create::CREATE_LLM_GATEWAY_API_KEYS_INDEX, "llm_gateway_api_keys_index");
        init_schema_sql!(conn, create::CREATE_LLM_GATEWAY_RESPONSE_CACHE_INDEX, "llm_gateway_response_cache_index");
        init_schema_sql!(conn, create::CREATE_LLM_GATEWAY_ROUTE_ROLLUPS_INDEX, "llm_gateway_route_rollups_index");
        init_schema_sql!(conn, create::CREATE_LLM_GATEWAY_USAGE_ROLLUPS_INDEX, "llm_gateway_usage_rollups_index");
        init_schema_sql!(conn, create::CREATE_LLM_USER_TOOLS_ENDPOINTS_INDEX, "llm_user_tools_endpoints_index");
        init_schema_sql!(conn, create::CREATE_LLM_CREDENTIALS_UNIQUE_LABEL_INDEX, "llm_credentials_unique_label_index");
        init_schema_sql!(conn, create::CREATE_ELS_POLICIES, "els_policies");
        init_schema_sql!(conn, create::CREATE_ELS_POLICY_ASSIGNMENTS, "els_policy_assignments");
        init_schema_sql!(conn, create::CREATE_ELS_POLICY_VERSIONS, "els_policy_versions");
        init_schema_sql!(conn, create::CREATE_ELS_POLICY_POINTERS, "els_policy_pointers");
        init_schema_sql!(conn, create::CREATE_ORG_KEY_REFS, "org_key_refs");
        init_schema_sql!(conn, create::CREATE_ENCRYPTION_KEYS, "encryption_keys");

        Ok(())
    }

    cfg_if::cfg_if! {
        if #[cfg(embedded_db)] {
            /// Get a Postgres connection in embedded-db mode.
            pub async fn pg_connection(&self) -> ResultEP<TursoConnection> {
                self.postgres_pool.as_turso_pool().connect()
            }
        } else {
            /// Get a Postgres connection.
            pub async fn pg_connection<'a>(&'a self) -> ResultEP<PooledConnection<'a, PostgresConnectionManager<NoTls>>> {
                self.postgres_pool.get_conn().await
            }
        }
    }

    /// Get a Clickhouse connection from the internal pool.
    pub async fn clickhouse_connection(&self) -> ResultEP<ClickhousePooledConnection> {
        self.clickhouse_pool.get_conn().await
    }

    /// Get the internal Clickhouse pool.
    pub fn clickhouse_pool(&self) -> &C {
        &self.clickhouse_pool
    }

    /// Get the cache TTL in seconds.
    pub fn cache_ttl(&self) -> u64 {
        self.cache_ttl.as_secs()
    }

    /// Get organization UUID from string
    pub async fn get_organization_uuid_from_string(
        &self,
        string: String,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<OrganizationUuid> {
        match Uuid::parse_str(&string) {
            Ok(uuid) => Ok(OrganizationUuid::from(uuid)),
            Err(_) => <Self as CacheIdFunctions<OrganizationSchema, OrganizationCacheId>>::get_from_cache(
                self,
                &OrganizationCacheId::new(None, OrganizationId::new(string)),
                telemetry_wrapper,
            )
            .await
            .map(|schema| schema.uuid()),
        }
    }
}

cfg_if::cfg_if! {
    if #[cfg(all(test, embedded_db))] {
        pub mod mocks {}
    } else if #[cfg(test)] {
pub mod mocks {
    use super::*;
    use std::sync::Mutex as StdMutex;

    // Mock internal cache connection for testing. Name retained for compatibility.
    #[derive(Clone)]
    pub struct MockRedisConnection {
        cache: InternalCache,
    }

    impl MockRedisConnection {
        pub fn new(_connection_error: bool) -> Self {
            Self { cache: InternalCache::new() }
        }
    }

    impl EdenRedisConnection for MockRedisConnection {
        fn internal_cache(&self) -> InternalCache {
            self.cache.clone()
        }
    }

    // Mock Postgres Connection for testing
    pub struct MockPostgresConnection {
        executed_queries: StdMutex<Vec<String>>,
        should_fail: bool,
    }

    impl MockPostgresConnection {
        pub fn new(should_fail: bool) -> Self {
            Self { executed_queries: StdMutex::new(Vec::new()), should_fail }
        }

        pub fn get_executed_queries(&self) -> Vec<String> {
            self.executed_queries.lock().expect("Poisoned mutex").clone()
        }
    }

    impl EdenPostgresConnection for MockPostgresConnection {
        async fn get_conn(&self) -> ResultEP<PooledConnection<'_, PostgresConnectionManager<NoTls>>> {
            if self.should_fail {
                Err(EpError::database("Simulated Postgres connection error"))
            } else {
                // This won't be called in tests that verify error handling
                unimplemented!("This is a mock - not meant to be called with should_fail=false")
            }
        }

        async fn execute(&self, query: &str, _params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> ResultEP<u64> {
            if self.should_fail {
                Err(EpError::database("Simulated query execution error"))
            } else {
                // Record the query for verification
                self.executed_queries.lock().expect("Poisoned mutex").push(format!("execute:{query}"));
                Ok(1) // Simulate one row affected
            }
        }

        async fn batch_execute(&self, query: &str) -> ResultEP<()> {
            if self.should_fail {
                Err(EpError::database("Simulated batch execution error"))
            } else {
                // Record the query for verification
                self.executed_queries.lock().expect("Poisoned mutex").push(format!("batch_execute:{query}"));
                Ok(())
            }
        }
    }

    // Mock Clickhouse Connection for testing
    pub struct MockClickhouseConnection {
        connection_error: bool,
    }

    impl MockClickhouseConnection {
        pub fn new(connection_error: bool) -> Self {
            Self { connection_error }
        }
    }

    impl EdenClickhouseConnection for MockClickhouseConnection {
        async fn get_conn(&self) -> ResultEP<ClickhousePooledConnection> {
            if self.connection_error {
                Err(EpError::database("Simulated Clickhouse connection error"))
            } else {
                unimplemented!("This is a mock - not meant to be called with connection_error=false")
            }
        }
    }
}
    }
}

cfg_if::cfg_if! {
    if #[cfg(all(test, embedded_db))] {
        mod tests {}
    } else if #[cfg(test)] {
mod tests {
    use super::*;
    use crate::db::lib::mocks::{MockClickhouseConnection, MockPostgresConnection, MockRedisConnection};
    #[cfg(feature = "infra-tests")]
    use crate::test_utils::database_test_utils::create_database_manager;
    #[cfg(feature = "infra-tests")]
    use tokio_postgres::Row;

    #[tokio::test]
    async fn test_database_manager_with_mocks() {
        // Create mock cache handles
        let mock_redis = MockRedisConnection::new(false);
        let mock_redis_rbac = MockRedisConnection::new(false);

        // Create mock Postgres connection
        let mock_postgres = MockPostgresConnection::new(false);
        let mock_clickhouse = MockClickhouseConnection::new(false);

        // Create DatabaseManager with mocks
        let db_manager = DatabaseManager::new_with_connections(
            mock_redis,
            mock_redis_rbac,
            mock_postgres,
            mock_clickhouse,
            CacheTtl::from_secs(3600), // 1 hour TTL
            None,                      // No JWT for test
        );

        // Now use db_manager in tests
        assert_eq!(db_manager.cache_ttl(), 3600);

        // You can add more assertions and tests here
    }

    #[tokio::test]
    async fn test_database_initialization() {
        // Create mock Postgres that records executed queries
        let mock_postgres = MockPostgresConnection::new(false);

        // Run the initialization
        let result =
            DatabaseManager::<MockRedisConnection, MockPostgresConnection, MockClickhouseConnection>::initialize_database(&mock_postgres)
                .await;

        // Verify initialization succeeded
        assert!(result.is_ok());

        // Get the executed queries and verify them
        let executed_queries = mock_postgres.get_executed_queries();

        assert!(
            executed_queries.iter().all(|q| q.starts_with("batch_execute:")),
            "database initialization should use batch_execute for all static schema SQL"
        );

        // Assert that all expected tables were created
        assert!(executed_queries.iter().any(|q| q.contains("CREATE TABLE IF NOT EXISTS endpoints")));
        assert!(executed_queries.iter().any(|q| q.contains("CREATE TABLE IF NOT EXISTS templates")));
        assert!(executed_queries.iter().any(|q| q.contains("CREATE TABLE IF NOT EXISTS organizations")));
        assert!(executed_queries.iter().any(|q| q.contains("CREATE TABLE IF NOT EXISTS execution_runs")));
        assert!(executed_queries.iter().any(|q| q.contains("CREATE TABLE IF NOT EXISTS evidence_records")));
        assert!(executed_queries.iter().any(|q| q.contains("CREATE TABLE IF NOT EXISTS trigger_events")));

        // Add more assertions as needed to verify all tables were created
    }

    #[cfg(feature = "infra-tests")]
    #[tokio::test]
    async fn test_initialize_database_adds_job_uuid_column() {
        let db_manager = create_database_manager().await;
        let conn = db_manager.pg_connection().await.expect("pg connection");

        let rows: Vec<Row> = conn
            .query(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'migrations' AND column_name = 'job_uuid';",
                &[],
            )
            .await
            .expect("query columns");

        assert!(!rows.is_empty(), "migrations.job_uuid column should exist after initialization");
    }

    #[cfg(feature = "infra-tests")]
    #[tokio::test]
    async fn test_initialize_database_creates_execution_runs_table() {
        let db_manager = create_database_manager().await;
        let conn = db_manager.pg_connection().await.expect("pg connection");

        let rows: Vec<Row> = conn
            .query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'execution_runs';",
                &[],
            )
            .await
            .expect("query execution_runs table");

        assert!(!rows.is_empty(), "execution_runs table should exist after initialization");
    }

    #[tokio::test]
    async fn test_error_handling() {
        // Create mock cache handles. The internal ShardMap cache is always local.
        let mock_redis = MockRedisConnection::new(true);
        let mock_redis_rbac = MockRedisConnection::new(true);

        // Create mock Postgres with simulated failure
        let mock_postgres = MockPostgresConnection::new(true);
        let mock_clickhouse = MockClickhouseConnection::new(true);

        // Create DatabaseManager with mocks
        let db_manager = DatabaseManager::new_with_connections(
            mock_redis,
            mock_redis_rbac,
            mock_postgres,
            mock_clickhouse,
            CacheTtl::from_secs(3600),
            None,
        );

        // Cache access remains available even when backing databases fail.
        let result = db_manager.cache_connection().await;
        assert!(result.is_ok());

        // Test a method that should fail due to Postgres connection error
        let result = db_manager.pg_connection().await;
        assert!(result.is_err());
        // assert!(
        //     result
        //         .expect("failed to get the result")
        //         .to_string()
        //         .contains("Simulated Postgres connection error")
        // );
    }

    #[cfg(feature = "infra-tests")]
    #[tokio::test]
    async fn test_real_connections_basic() {
        let database_manager = create_database_manager().await;

        // Try to get an internal cache handle
        let cache_result = database_manager.cache_connection().await;
        assert!(cache_result.is_ok(), "Failed to get internal cache handle: {:?}", cache_result.err());

        // Try to get a Postgres connection
        let pg_result = database_manager.pg_connection().await;
        assert!(pg_result.is_ok(), "Failed to get Postgres connection: {:?}", pg_result.err());

        println!("✓ Cache and Postgres connections successful");
    }
}
    }
}
