// Analytics Demo - High-Performance Redis Migration Showcase
//
// Re-architected for Redis-only hot path demonstration.
// Postgres is optional and only used if explicitly enabled for seeding.
// All runtime traffic goes through Redis only.

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
mod validation;
mod workers;

use config::Config;
use database::RedisCache;
use generators::DataGenerator;
use metrics::AppMetrics;
use validation::DataValidator;
use workers::{
    QuerySimulatorWorker, CacheWarmupWorker, EventSimulatorWorker,
    SystemMonitorWorker, OrgIdCache
};

#[derive(Clone)]
struct AppState {
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
    validator: Arc<DataValidator>,
    config: Arc<Config>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("analytics_demo=info,sqlx=warn")
        .init();

    let config = Config::parse();
    info!("Starting Redis-only analytics demo");
    info!("Configuration:");
    info!("  - Target QPS: {}", config.queries_per_second);
    info!("  - Events/sec: {}", config.events_per_second);
    info!("  - Organizations: {}", config.organizations);
    info!("  - Max workers: {}", config.max_workers);
    info!("  - Redis pool size: {}", config.redis_pool_size);
    info!("  - Validation sample rate: {:.1}%", config.validation_sample_rate * 100.0);
    info!("  - Mode: Redis-only (no Postgres in hot path)");

    // Initialize Redis cache
    let cache = Arc::new(RedisCache::new(&config.redis_url, config.redis_pool_size).await?);
    let metrics = Arc::new(AppMetrics::new());
    let generator = Arc::new(DataGenerator::new());
    let org_cache = Arc::new(OrgIdCache::new());
    let validator = Arc::new(DataValidator::new(config.validation_sample_rate, metrics.clone()));

    // Initialize synthetic org/user data (no DB needed)
    info!("Initializing synthetic organization data...");
    org_cache.initialize_synthetic(config.organizations, config.users_per_org).await;

    let state = AppState {
        cache: cache.clone(),
        metrics: metrics.clone(),
        generator: generator.clone(),
        org_cache: org_cache.clone(),
        validator: validator.clone(),
        config: Arc::new(config.clone()),
    };

    // Start cache warmup (populates Redis with synthetic data)
    tokio::spawn(start_cache_warmup(state.clone()));

    // Start event simulator (Redis INCR operations only)
    tokio::spawn(start_event_simulator(state.clone()));

    // Start query simulator (Redis GET/SET only)
    tokio::spawn(start_query_simulator(state.clone()));

    // Start system monitor
    tokio::spawn(start_system_monitor(state.clone()));

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&config.bind_address).await?;

    info!("===========================================");
    info!("Analytics Demo Ready (Redis-Only Mode)");
    info!("===========================================");
    info!("Metrics endpoint: http://{}/metrics", config.bind_address);
    info!("Health endpoint: http://{}/health", config.bind_address);
    info!("Target throughput: {} QPS", config.queries_per_second);
    info!("Simulated events: {}/sec", config.events_per_second);
    info!("Organizations: {}", config.organizations);
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

async fn start_cache_warmup(state: AppState) {
    let worker = CacheWarmupWorker::new(
        state.cache,
        state.metrics,
        state.generator,
        state.org_cache,
    );

    // Initial bulk population
    sleep(Duration::from_secs(1)).await;
    info!("Starting initial cache population...");
    if let Err(e) = worker.bulk_populate().await {
        error!("Bulk cache population error: {}", e);
    }

    // Periodic refresh (much less frequent since no DB)
    loop {
        sleep(Duration::from_secs(state.config.warmup_interval)).await;
        if let Err(e) = worker.warmup_refresh().await {
            error!("Cache warmup error: {}", e);
        }
    }
}

async fn start_event_simulator(state: AppState) {
    let worker = EventSimulatorWorker::new(
        state.cache,
        state.metrics,
        state.generator,
        state.org_cache,
    );

    // Wait for cache warmup
    sleep(Duration::from_secs(3)).await;

    loop {
        if let Err(e) = worker.run_batch(state.config.events_per_second).await {
            error!("Event simulator error: {}", e);
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn start_query_simulator(state: AppState) {
    let worker = QuerySimulatorWorker::new(
        state.cache,
        state.metrics,
        state.generator.clone(),
        state.org_cache,
        state.validator,
    );

    // Wait for cache warmup
    sleep(Duration::from_secs(3)).await;

    // Start worker pool
    worker.start_worker_pool(
        state.config.queries_per_second,
        state.config.organizations,
        state.config.max_workers,
    ).await;

    // Keep task alive
    loop {
        sleep(Duration::from_secs(3600)).await;
    }
}

async fn start_system_monitor(state: AppState) {
    let worker = SystemMonitorWorker::new(state.metrics, state.org_cache);

    loop {
        if let Err(e) = worker.update_system_metrics(&state.config).await {
            error!("System monitor error: {}", e);
        }
        sleep(Duration::from_secs(10)).await;
    }
}