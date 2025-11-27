// Analytics Demo - High-Performance Database Migration Showcase
//
// Enhanced for 10K+ QPS with diverse cache keys, Redis pooling, and no worker limits
// FIXED: Uses OrgIdCache for efficient org_id lookups, proper error logging

use anyhow::Result;
use axum::{extract::State, http::StatusCode, response::Response, routing::get, Router};
use clap::Parser;
use prometheus::{Encoder, TextEncoder};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{info, error};

mod config;
mod database;
mod generators;
mod metrics;
mod models;
mod workers;

use config::Config;
use database::{Database, RedisCache};
use generators::DataGenerator;
use metrics::AppMetrics;
use workers::{EventGeneratorWorker, QuerySimulatorWorker, CacheWarmupWorker, SystemMonitorWorker, OrgIdCache};

#[derive(Clone)]
struct AppState {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("analytics_demo=info,sqlx=warn")
        .init();

    let config = Config::parse();
    info!("Starting high-performance analytics demo");
    info!("Configuration:");
    info!("  - Target QPS: {}", config.queries_per_second);
    info!("  - Events/sec: {}", config.events_per_second);
    info!("  - Organizations: {}", config.organizations);
    info!("  - Max workers: {}", config.max_workers);
    info!("  - DB pool size: {}", config.db_pool_size);
    info!("  - Redis pool size: {}", config.redis_pool_size);

    let database = Arc::new(Database::new(&config.database_url, config.db_pool_size).await?);
    let cache = Arc::new(RedisCache::new(&config.redis_url, config.redis_pool_size).await?);
    let metrics = Arc::new(AppMetrics::new());
    let generator = Arc::new(DataGenerator::new());
    let org_cache = Arc::new(OrgIdCache::new());

    database.setup_schema().await?;
    database.seed_initial_data(&generator, &config).await?;

    // Initial cache population
    info!("Populating organization ID cache...");
    if let Err(e) = org_cache.refresh(&database, config.organizations).await {
        error!("Failed to populate org cache: {}", e);
        return Err(e);
    }
    info!("Organization ID cache populated with {} orgs", org_cache.get_org_ids().await.len());

    let state = AppState {
        database: database.clone(),
        cache: cache.clone(),
        metrics: metrics.clone(),
        generator: generator.clone(),
        org_cache: org_cache.clone(),
    };

    // Start org cache refresh task
    tokio::spawn(start_org_cache_refresh(state.clone(), config.clone()));

    // Start event generator
    tokio::spawn(start_event_generator(state.clone(), config.clone()));

    // Start query simulator with enhanced worker pool
    tokio::spawn(start_query_simulator(state.clone(), config.clone()));

    // Start cache warmup
    tokio::spawn(start_cache_warmup(state.clone(), config.clone()));

    // Start system monitor
    tokio::spawn(start_system_monitor(state.clone(), config.clone()));

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&config.bind_address).await?;

    info!("===========================================");
    info!("Analytics Demo Ready");
    info!("===========================================");
    info!("Metrics endpoint: http://{}/metrics", config.bind_address);
    info!("Health endpoint: http://{}/health", config.bind_address);
    info!("Target throughput: {} QPS", config.queries_per_second);
    info!("Expected cache hit ratio: ~{}%", config.cache_hit_target);
    info!("===========================================");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn metrics_handler(State(state): State<AppState>) -> Result<Response, StatusCode> {
    let encoder = TextEncoder::new();
    let metric_families = state.metrics.registry.gather();

    match encoder.encode_to_string(&metric_families) {
        Ok(output) => Ok(Response::builder()
            .header("content-type", encoder.format_type())
            .body(output.into())
            .unwrap()),
        Err(e) => {
            error!("Failed to encode metrics: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn start_org_cache_refresh(state: AppState, config: Config) {
    // Refresh org cache every 30 seconds
    loop {
        sleep(Duration::from_secs(30)).await;
        if let Err(e) = state.org_cache.refresh(&state.database, config.organizations).await {
            error!("Failed to refresh org cache: {}", e);
        }
    }
}

async fn start_event_generator(state: AppState, config: Config) {
    let worker = EventGeneratorWorker::new(
        state.database,
        state.cache,
        state.metrics,
        state.generator,
        state.org_cache,
    );

    loop {
        if let Err(e) = worker.run_batch(config.events_per_second, config.organizations).await {
            error!("Event generator error: {}", e);
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn start_query_simulator(state: AppState, config: Config) {
    let worker = QuerySimulatorWorker::new(
        state.database,
        state.cache,
        state.metrics,
        state.generator.clone(),
        state.org_cache,
    );

    // Start worker pool with no upper limit
    worker.start_worker_pool(
        config.queries_per_second,
        config.organizations,
        config.max_workers,
    ).await;

    // Keep task alive
    loop {
        sleep(Duration::from_secs(3600)).await;
    }
}

async fn start_cache_warmup(state: AppState, config: Config) {
    let worker = CacheWarmupWorker::new(
        state.database,
        state.cache,
        state.metrics,
        state.generator,
        state.org_cache,
    );

    // Initial bulk population after 2 seconds
    sleep(Duration::from_secs(2)).await;

    info!("Starting initial bulk cache population...");
    if let Err(e) = worker.bulk_populate().await {
        error!("Bulk cache population error: {}", e);
    }

    // Then periodic refresh
    loop {
        sleep(Duration::from_secs(config.warmup_interval)).await;
        if let Err(e) = worker.warmup_popular_queries(config.organizations).await {
            error!("Cache warmup error: {}", e);
        }
    }
}

async fn start_system_monitor(state: AppState, config: Config) {
    let worker = SystemMonitorWorker::new(state.database, state.metrics, state.org_cache);

    loop {
        if let Err(e) = worker.update_system_metrics(&config).await {
            error!("System monitor error: {}", e);
        }
        sleep(Duration::from_secs(10)).await;
    }
}