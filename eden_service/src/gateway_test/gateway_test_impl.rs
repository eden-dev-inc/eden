use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use crate::test_utils::redis_migrate_test_utils::{connect_to_interlay, connect_to_multi_redis, run_worker, start_interlay};

#[tokio::test]
#[ignore]
async fn test_proxy() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (endpoints, engine_service, database_manager, organization_schema, test_telemetry) = connect_to_multi_redis(1).await;
    let database_manager = Arc::new(database_manager);

    let origin_endpoint = endpoints[0].1.clone();
    let origin_schema = &endpoints[0].2;

    const INTERLAY_PORT: u16 = 5252;

    let _interlay_handles = start_interlay(
        INTERLAY_PORT,
        origin_schema.clone(),
        origin_endpoint,
        organization_schema.clone(),
        engine_service.clone(),
        database_manager.clone(),
        None,
        test_telemetry.clone(),
    )
    .await;

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let interlay_endpoint =
        connect_to_interlay(INTERLAY_PORT, organization_schema, engine_service.clone(), database_manager, test_telemetry.clone()).await;

    const NWORKERS: usize = 50000;
    const DURATION_S: u64 = 5;
    let mut workers = Vec::with_capacity(NWORKERS);
    let mut shutdown_tx = Vec::with_capacity(NWORKERS);
    for _ in 0..NWORKERS {
        let (worker_shutdown_tx, worker_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        workers.push(tokio::spawn(run_worker(
            worker_shutdown_rx,
            engine_service.clone(),
            interlay_endpoint.clone(),
            test_telemetry.clone(),
        )));
        shutdown_tx.push(worker_shutdown_tx);
    }
    sleep(Duration::from_secs(DURATION_S)).await;
    while let Some(tx) = shutdown_tx.pop() {
        tx.send(()).unwrap_or_else(|_| eprintln!("couldn't send oneshot worker shutdown"));
    }
    let mut max_counter = 0;
    let mut joined_workers = 0;
    while let Some(worker) = workers.pop() {
        let counter = worker.await.unwrap_or_default();
        if counter > max_counter {
            max_counter = counter;
        }
        joined_workers += 1;
    }
    println!("Last counter: {max_counter}");
    assert_eq!(NWORKERS, joined_workers);
    assert!(max_counter > NWORKERS as i64);
}
