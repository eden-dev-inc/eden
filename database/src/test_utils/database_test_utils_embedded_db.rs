use crate::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use crate::test_utils::embedded_db_test_utils::{create_local_database_manager, create_local_database_manager_at_path};
use eden_core::auth::Jwt;
use std::time::Duration;
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_postgres::NoTls;

pub async fn create_database_manager() -> DatabaseManager<RedisConn, PgConn, ClickhouseConn> {
    create_local_database_manager().await
}

fn dedicated_local_db_path() -> String {
    format!("/tmp/eden_dedicated_{}.db", uuid::Uuid::new_v4())
}

pub async fn create_database_manager_dedicated() -> (
    ContainerAsync<Redis>,
    ContainerAsync<Postgres>,
    ContainerAsync<ClickHouse>,
    DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
) {
    // Keep real target services available for endpoint/integration tests while the
    // control-plane DatabaseManager itself remains embedded-db and file-backed.
    let (redis_container, _redis_url) = create_redis().await;
    let (pg_container, _pg_url) = create_postgres().await;
    let (clickhouse_container, _clickhouse_url) = create_clickhouse().await;
    let db_manager = create_local_database_manager_at_path(&dedicated_local_db_path(), None).await;
    (redis_container, pg_container, clickhouse_container, db_manager)
}

pub async fn build_database_manager(
    _redis_url: &str,
    control_plane_db_path: &str,
    _clickhouse_url: &str,
    jwt: Option<Jwt>,
) -> DatabaseManager<RedisConn, PgConn, ClickhouseConn> {
    let control_plane_db_path = control_plane_db_path.trim();
    assert!(
        !control_plane_db_path.is_empty(),
        "embedded_db build_database_manager requires a control-plane database path",
    );
    assert!(
        !control_plane_db_path.contains("://"),
        "embedded_db build_database_manager expected a local control-plane database path, got URI-like input: {}",
        control_plane_db_path,
    );

    create_local_database_manager_at_path(control_plane_db_path, jwt).await
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

pub async fn create_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = match testcontainers_modules::postgres::Postgres::default().start().await {
        Ok(db) => db,
        Err(e) => panic!("Postgres Container: {}", e),
    };

    let host_port = match container.get_host_port_ipv4(5432).await {
        Ok(host_port) => host_port,
        Err(e) => panic!("Postgres Host: {}", e),
    };

    println!("postgres port: {}", host_port);

    let connection = format!("postgres://postgres:postgres@127.0.0.1:{host_port}/postgres");
    wait_for_postgres_ready(&connection).await;
    (container, connection)
}

pub async fn create_clickhouse() -> (ContainerAsync<ClickHouse>, String) {
    use crate::test_utils::clickhouse_test_utils::start_clickhouse_with_retry;

    let container = start_clickhouse_with_retry().await;

    let host_ip = container.get_host().await.expect("Failed to get ClickHouse host");
    let host_port = container.get_host_port_ipv4(8123).await.expect("Failed to get ClickHouse port");

    println!("clickhouse port: {}", host_port);

    let ch_url = format!("http://{host_ip}:{host_port}/ping");
    crate::test_utils::clickhouse_test_utils::wait_for_clickhouse_ready(&ch_url)
        .await
        .expect("ClickHouse failed to become ready");

    (container, format!("http://{host_ip}:{host_port}"))
}

pub async fn create_redis() -> (ContainerAsync<Redis>, String) {
    let container = match testcontainers_modules::redis::Redis::default().start().await {
        Ok(db) => db,
        Err(e) => panic!("Redis Container: {}", e),
    };

    crate::test_utils::redis_test_utils::wait_for_redis_ready(&container).await;

    let host_port = match container.get_host_port_ipv4(6379).await {
        Ok(host_port) => host_port,
        Err(e) => panic!("Redis Host: {}", e),
    };

    (container, format!("redis://127.0.0.1:{host_port}"))
}
