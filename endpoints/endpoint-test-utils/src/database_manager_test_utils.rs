use std::time::{Duration, Instant};

use crate::{DEFAULT_REDIS_STACK_VERSION, DEFAULT_REDIS_VERSION};
#[cfg(not(embedded_db))]
use database::db::lib::{CacheTtl, ClickhouseConn, ClickhouseDbConfig, DEFAULT_CLICKHOUSE_POOL_SIZE, DatabaseManager, PgConn, RedisConn};
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::{CmdWaitFor, ExecCommand, IntoContainerPort};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub async fn wait_for_clickhouse_ready(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Waiting for ClickHouse to be ready...");
    let t0 = Instant::now();
    let client = reqwest::Client::new();
    let max_retries = 120;
    let retry_delay = Duration::from_millis(500);

    for attempt in 0..max_retries {
        match client.get(url).timeout(Duration::from_secs(2)).send().await {
            Ok(resp) if resp.status().is_success() => {
                println!("ClickHouse ready: {} ms (attempt {})", t0.elapsed().as_millis(), attempt + 1);
                return Ok(());
            }
            _ => {
                if attempt < max_retries - 1 {
                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    }

    Err(format!("ClickHouse failed to become ready after {} ms", t0.elapsed().as_millis()).into())
}

#[cfg(not(embedded_db))]
pub async fn create_database_manager() -> (
    ContainerAsync<GenericImage>,
    ContainerAsync<Postgres>,
    ContainerAsync<ClickHouse>,
    DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
) {
    let (redis_container, host, port) = initialize_redis(None).await;
    let (pg_container, pg_connection) = create_postgres(None).await;
    let (clickhouse_container, clickhouse_url) = create_clickhouse().await;
    let clickhouse_config =
        ClickhouseDbConfig::new(clickhouse_url, None, None, None, DEFAULT_CLICKHOUSE_POOL_SIZE).expect("Failed to build Clickhouse config");

    match DatabaseManager::<RedisConn, PgConn, ClickhouseConn>::new(
        &format!("redis://{host}:{port}"),
        &pg_connection,
        clickhouse_config,
        CacheTtl::from_secs(36000),
        None,
    )
    .await
    {
        Ok(db) => (redis_container, pg_container, clickhouse_container, db),
        Err(e) => panic!("Failed to create Database Manager: {}", e),
    }
}

pub async fn create_clickhouse() -> (ContainerAsync<ClickHouse>, String) {
    use database::test_utils::clickhouse_test_utils::start_clickhouse_with_retry;

    let container = start_clickhouse_with_retry().await;
    let host_ip = container.get_host().await.expect("Failed to get ClickHouse host");
    let host_port = container.get_host_port_ipv4(8123).await.expect("Failed to get ClickHouse port");

    println!("clickhouse port: {}", host_port);

    let ch_url = format!("http://{host_ip}:{host_port}/ping");
    database::test_utils::clickhouse_test_utils::wait_for_clickhouse_ready(&ch_url)
        .await
        .expect("ClickHouse failed to become ready");

    (container, format!("http://{host_ip}:{host_port}"))
}

pub async fn create_postgres(port: Option<u16>) -> (ContainerAsync<Postgres>, String) {
    let container = testcontainers_modules::postgres::Postgres::default();
    let container = match if let Some(port) = port {
        container.with_mapped_port(port, 5432.tcp()).start().await
    } else {
        container.start().await
    } {
        Ok(db) => db,
        Err(e) => panic!("Postgres Container: {}", e),
    };

    println!("Waiting for Postgres to get ready...");
    let t0 = Instant::now();
    container
        .exec(ExecCommand::new(["pg_isready", "-t", "10"]).with_cmd_ready_condition(CmdWaitFor::exit_code(0)))
        .await
        .unwrap();
    println!("Postgres ready: {} ms", t0.elapsed().as_millis());

    let host_port = match container.get_host_port_ipv4(5432).await {
        Ok(host_port) => host_port,
        Err(e) => panic!("Postgres Host: {}", e),
    };

    println!("postgres port: {}", host_port);

    (container, format!("postgres://postgres:postgres@127.0.0.1:{host_port}/postgres"))
}

pub async fn initialize_redis(version: Option<&str>) -> (ContainerAsync<GenericImage>, String, u16) {
    let container = match testcontainers_modules::testcontainers::GenericImage::new("redis", version.unwrap_or(DEFAULT_REDIS_VERSION))
        .start()
        .await
    {
        Ok(db) => db,
        Err(e) => panic!("Redis Container: {}", e),
    };

    wait_for_redis_ready(&container).await;

    let host_ip = container.get_host().await.expect("Failed to get host address");
    let host_port = container.get_host_port_ipv4(6379).await.expect("Failed to get host port");

    (container, host_ip.to_string(), host_port)
}

pub async fn initialize_redis_stack(version: Option<&str>) -> (ContainerAsync<GenericImage>, String, u16) {
    let container = match testcontainers_modules::testcontainers::GenericImage::new(
        "redis/redis-stack-server",
        version.unwrap_or(DEFAULT_REDIS_STACK_VERSION),
    )
    .start()
    .await
    {
        Ok(db) => db,
        Err(e) => panic!("Redis Stack Server Container: {}", e),
    };

    wait_for_redis_ready(&container).await;

    let host_ip = container.get_host().await.expect("Failed to get host address");
    let host_port = container.get_host_port_ipv4(6379).await.expect("Failed to get host port");

    (container, host_ip.to_string(), host_port)
}

pub async fn wait_for_redis_ready<I: testcontainers_modules::testcontainers::Image>(container: &ContainerAsync<I>) {
    println!("Waiting for Redis to be ready...");
    let t0 = Instant::now();
    container
        .exec(ExecCommand::new(["redis-cli", "ping"]).with_cmd_ready_condition(CmdWaitFor::message_on_stdout("PONG")))
        .await
        .expect("Redis not ready");
    println!("Redis ready: {} ms", t0.elapsed().as_millis());
}
