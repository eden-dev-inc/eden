use crate::lib::{CacheTtl, ClickhouseConn, ClickhouseDbConfig, DEFAULT_CLICKHOUSE_POOL_SIZE, DatabaseManager, PgConn, RedisConn};
use eden_core::auth::Jwt;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::{Mutex as AsyncMutex, OnceCell};
use tokio_postgres::NoTls;

#[allow(dead_code)]
struct SharedContainers {
    _pg_container: Mutex<Option<ContainerAsync<Postgres>>>,
    _ch_container: Mutex<Option<ContainerAsync<ClickHouse>>>,
    pg_url: String,
    ch_url: String,
}

static SHARED: OnceCell<SharedContainers> = OnceCell::const_new();
static MIGRATION_COMPATIBILITY_TEST_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
static MIGRATION_COMPATIBILITY_TEST_DB: OnceCell<Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>> = OnceCell::const_new();

pub fn migration_compatibility_test_lock() -> &'static AsyncMutex<()> {
    MIGRATION_COMPATIBILITY_TEST_LOCK.get_or_init(|| AsyncMutex::new(()))
}

pub async fn shared_migration_compatibility_database_manager() -> Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>> {
    MIGRATION_COMPATIBILITY_TEST_DB.get_or_init(|| async { Arc::new(create_database_manager().await) }).await.clone()
}

async fn shared_containers() -> &'static SharedContainers {
    SHARED
        .get_or_init(|| async {
            let (pg_container, pg_url) = create_postgres().await;
            let (ch_container, ch_url) = create_clickhouse().await;
            SharedContainers {
                _pg_container: Mutex::new(Some(pg_container)),
                _ch_container: Mutex::new(Some(ch_container)),
                pg_url,
                ch_url,
            }
        })
        .await
}

#[cfg(test)]
#[ctor::dtor]
fn shutdown_shared_containers() {
    let Some(shared) = SHARED.get() else { return };
    let pg = shared._pg_container.lock().expect("lock").take();
    let ch = shared._ch_container.lock().expect("lock").take();
    if pg.is_none() && ch.is_none() {
        return;
    }
    let _ = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("teardown runtime");
        rt.block_on(async {
            if let Some(c) = pg {
                let _ = c.rm().await;
            }
            if let Some(c) = ch {
                let _ = c.rm().await;
            }
        });
    })
    .join();
}

#[derive(Clone)]
struct SearchPathCustomizer {
    schema: String,
}

impl fmt::Debug for SearchPathCustomizer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SearchPathCustomizer").field("schema", &self.schema).finish()
    }
}

impl bb8::CustomizeConnection<tokio_postgres::Client, tokio_postgres::Error> for SearchPathCustomizer {
    fn on_acquire<'a>(
        &'a self,
        connection: &'a mut tokio_postgres::Client,
    ) -> Pin<Box<dyn Future<Output = Result<(), tokio_postgres::Error>> + Send + 'a>> {
        let sql = format!("SET search_path TO {}, public", self.schema);
        Box::pin(async move {
            connection.batch_execute(&sql).await?;
            Ok(())
        })
    }
}

pub async fn create_database_manager() -> DatabaseManager<RedisConn, PgConn, ClickhouseConn> {
    let shared = shared_containers().await;
    let schema_name = format!("test_{}", uuid::Uuid::new_v4().as_simple());

    {
        let (client, conn) = tokio_postgres::connect(&shared.pg_url, NoTls).await.expect("temp pg connect");
        let driver = tokio::spawn(async move {
            let _ = conn.await;
        });
        client
            .batch_execute(&format!("CREATE SCHEMA {schema_name}; SET search_path TO {schema_name};"))
            .await
            .expect("create test schema");
        let pool = create_postgres_connection_with_schema(&shared.pg_url, &schema_name).await.expect("schema pg pool");
        DatabaseManager::<RedisConn, PgConn, ClickhouseConn>::initialize_database(&pool).await.expect("initialize test schema");
        driver.abort();
    }

    let pg_pool = create_postgres_connection_with_schema(&shared.pg_url, &schema_name).await.expect("pg pool with schema");
    let internal_cache = RedisConn::new();
    let ch_config =
        ClickhouseDbConfig::new(shared.ch_url.clone(), None, None, None, DEFAULT_CLICKHOUSE_POOL_SIZE).expect("clickhouse config");
    let ch_pool = crate::lib::create_clickhouse_connection(&ch_config).expect("clickhouse pool");

    let mut db =
        DatabaseManager::new_with_connections(internal_cache.clone(), internal_cache, pg_pool, ch_pool, CacheTtl::from_secs(36000), None);
    db.pg_url = shared.pg_url.clone();
    db
}

async fn create_postgres_connection_with_schema(connection_string: &str, schema: &str) -> Result<PgConn, eden_core::error::EpError> {
    let manager = bb8_postgres::PostgresConnectionManager::new_from_stringlike(connection_string, NoTls)
        .map_err(|e| eden_core::error::EpError::database(format!("pg manager: {e}")))?;

    bb8::Pool::builder()
        .max_size(10)
        .connection_timeout(Duration::from_secs(30))
        .connection_customizer(Box::new(SearchPathCustomizer { schema: schema.to_string() }))
        .build(manager)
        .await
        .map_err(|e| eden_core::error::EpError::database(format!("pg pool: {e}")))
}

pub async fn create_database_manager_dedicated() -> (
    ContainerAsync<Redis>,
    ContainerAsync<Postgres>,
    ContainerAsync<ClickHouse>,
    DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
) {
    let (redis_container, redis_url) = create_redis().await;
    let (pg_container, pg_url) = create_postgres().await;
    let (ch_container, ch_url) = create_clickhouse().await;
    let ch_config = ClickhouseDbConfig::new(ch_url, None, None, None, DEFAULT_CLICKHOUSE_POOL_SIZE).expect("clickhouse config");

    match DatabaseManager::<RedisConn, PgConn, ClickhouseConn>::new(&redis_url, &pg_url, ch_config, CacheTtl::from_secs(36000), None).await
    {
        Ok(db) => (redis_container, pg_container, ch_container, db),
        Err(e) => panic!("Failed to create dedicated Database Manager: {}", e),
    }
}

pub async fn build_database_manager(
    redis_url: &str,
    pg_url: &str,
    clickhouse_url: &str,
    jwt: Option<Jwt>,
) -> DatabaseManager<RedisConn, PgConn, ClickhouseConn> {
    let ch_config =
        ClickhouseDbConfig::new(clickhouse_url.to_string(), None, None, None, DEFAULT_CLICKHOUSE_POOL_SIZE).expect("clickhouse config");

    DatabaseManager::<RedisConn, PgConn, ClickhouseConn>::new(redis_url, pg_url, ch_config, CacheTtl::from_secs(3600), jwt)
        .await
        .expect("Failed to create Database Manager")
}

async fn wait_for_postgres_ready(connection: &str) {
    let max_retries = 60;
    let retry_delay = Duration::from_millis(500);

    for attempt in 0..max_retries {
        if let Ok(Ok((client, connection_task))) =
            tokio::time::timeout(Duration::from_secs(2), tokio_postgres::connect(connection, NoTls)).await
        {
            let driver = tokio::spawn(async move {
                let _ = connection_task.await;
            });

            let health = tokio::time::timeout(Duration::from_secs(2), client.simple_query("SELECT 1")).await;
            driver.abort();

            if let Ok(Ok(_)) = health {
                return;
            }
        }

        if attempt < max_retries - 1 {
            tokio::time::sleep(retry_delay).await;
        }
    }

    panic!("PostgreSQL failed to become ready after {} retries (connection: {})", max_retries, connection);
}

pub async fn create_clickhouse() -> (ContainerAsync<ClickHouse>, String) {
    use super::clickhouse_test_utils::start_clickhouse_with_retry;

    let container = start_clickhouse_with_retry().await;

    let host_ip = container.get_host().await.expect("Failed to get ClickHouse host");
    let host_port = container.get_host_port_ipv4(8123).await.expect("Failed to get ClickHouse port");

    println!("clickhouse port: {}", host_port);

    let ch_url = format!("http://{host_ip}:{host_port}/ping");
    super::clickhouse_test_utils::wait_for_clickhouse_ready(&ch_url).await.expect("ClickHouse failed to become ready");

    (container, format!("http://{host_ip}:{host_port}"))
}

pub async fn create_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = match testcontainers_modules::postgres::Postgres::default().start().await {
        Ok(db) => db,
        Err(e) => panic!("Postgres Container: {}", e),
    };

    let host_port = match container.get_host_port_ipv4(5432).await {
        Ok(host_port) => host_port,
        Err(e) => panic!("Postgres Host: {}", e),
    };
    let host_ip = container.get_host().await.expect("Failed to get Postgres host");

    println!("postgres port: {}", host_port);

    let connection = format!("postgres://postgres:postgres@{host_ip}:{host_port}/postgres");
    wait_for_postgres_ready(&connection).await;
    (container, connection)
}

pub async fn create_redis() -> (ContainerAsync<Redis>, String) {
    let container = match testcontainers_modules::redis::Redis::default().start().await {
        Ok(db) => db,
        Err(e) => panic!("Redis Container: {}", e),
    };

    super::redis_test_utils::wait_for_redis_ready(&container).await;

    let host_port = match container.get_host_port_ipv4(6379).await {
        Ok(host_port) => host_port,
        Err(e) => panic!("Redis Host: {}", e),
    };
    let host_ip = container.get_host().await.expect("Failed to get Redis host");

    println!("redis port: {}", host_port);

    (container, format!("redis://{host_ip}:{host_port}"))
}
