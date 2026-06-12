use std::time::Instant;

use crate::{DEFAULT_REDIS_STACK_VERSION, DEFAULT_REDIS_VERSION};
use testcontainers_modules::testcontainers::core::{CmdWaitFor, ExecCommand};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

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
