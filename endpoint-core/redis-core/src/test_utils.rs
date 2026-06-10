#![allow(clippy::unwrap_used)]

use std::time::Instant;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::core::{CmdWaitFor, ExecCommand};
use tokio::time::{Duration, sleep};

/// Wait for a Redis container to report ready via redis-cli ping
pub async fn wait_for_redis_ready<I: testcontainers_modules::testcontainers::Image>(container: &ContainerAsync<I>) {
    println!("Waiting for Redis to be ready...");
    let t0 = Instant::now();
    let mut last_error = None;
    for _ in 0..50 {
        match container
            .exec(ExecCommand::new(["redis-cli", "ping"]).with_cmd_ready_condition(CmdWaitFor::message_on_stdout("PONG")))
            .await
        {
            Ok(_) => {
                println!("Redis ready: {} ms", t0.elapsed().as_millis());
                return;
            }
            Err(err) => {
                last_error = Some(err);
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("Redis not ready: {:?}", last_error);
}
