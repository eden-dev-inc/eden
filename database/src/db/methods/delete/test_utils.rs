// Add this in src/test_utils/mod.rs

#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]

use crate::db::lib::{
    CacheTtl, ClickhouseConn, ClickhouseDbConfig, DEFAULT_CLICKHOUSE_POOL_SIZE, DatabaseManager, EdenClickhouseConnection,
    EdenPostgresConnection, EdenRedisConnection, PgConn, RedisConn, ShardCache,
};
use eden_core::telemetry::TelemetryWrapper;
use std::env;
use uuid::Uuid;

pub async fn setup_test_db() -> DatabaseManager<RedisConn, PgConn, ClickhouseConn> {
    // Set up environment variables for test database
    unsafe {
        env::set_var("DB_HOST", "localhost");
        env::set_var("DB_PORT", "5432");
        env::set_var("DB_USER", "postgres");
        env::set_var("DB_PASSWORD", "postgres");
        env::set_var("DB_NAME", format!("test_db_{}", Uuid::new_v4()));
    }
    let clickhouse_config = ClickhouseDbConfig::new("http://localhost:8123".to_string(), None, None, None, DEFAULT_CLICKHOUSE_POOL_SIZE)
        .expect("Failed to build Clickhouse config");

    // Create test database manager
    let db: DatabaseManager<RedisConn, PgConn, ClickhouseConn> = DatabaseManager::<RedisConn, PgConn, ClickhouseConn>::new(
        "redis://localhost:6379",
        &format!(
            "postgres://{}:{}@{}:{}/{}",
            env::var("DB_USER").unwrap_or_default(),
            env::var("DB_PASSWORD").unwrap_or_default(),
            env::var("DB_HOST").unwrap_or_default(),
            env::var("DB_PORT").unwrap_or_default(),
            env::var("DB_NAME").unwrap_or_default()
        ),
        clickhouse_config,
        CacheTtl::from_secs(3600), // 1 hour TTL for test cache
        None,                      // No JWT for tests
    )
    .await
    .expect("Failed to create test database manager");

    db
}

pub async fn teardown_test_db<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let _ = db.internal_cache().clear_all().await;

    // Drop test database
    if let Ok(pg_conn) = db.pg_connection().await {
        let _ = pg_conn.execute(&format!("DROP DATABASE IF EXISTS {}", env::var("DB_NAME").unwrap_or_default()), &[]).await;
    }
}

// Helper function to create test data
pub async fn create_test_schema<R, P, C>(_db: &DatabaseManager<R, P, C>, _telemetry_wrapper: &TelemetryWrapper) -> Uuid {
    use ep_core::database::schema::organization::OrganizationSchema;
    let uuid = Uuid::new_v4();
    let _schema = OrganizationSchema::new("test-org".to_string(), None, Vec::new(), Some("Test Organization".to_string()));

    // Store in cache
    // let cache_key = OrganizationCacheUuid::new(None, uuid);

    uuid
}

// Helper to verify database state
pub async fn verify_db_state<R, P, C>(db: &DatabaseManager<R, P, C>, table: &str, uuid: Uuid, should_exist: bool) -> bool
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let conn = match db.pg_connection().await {
        Ok(conn) => conn,
        Err(_) => return false,
    };
    let result = conn.query_one(&format!("SELECT * FROM {} WHERE uuid = $1", table), &[&uuid]).await;

    result.is_ok() == should_exist
}

// Add a macro to create test schema structs
#[macro_export]
macro_rules! create_test_schema {
    ($schema_type:ty, $id:expr, $uuid:expr) => {{ <$schema_type>::new(Some($id.to_string()), $uuid, Some(format!("Test {}", $id)), Vec::new()) }};
}
