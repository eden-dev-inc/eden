// Analytics Demo - Database Migration Showcase
//
// This application simulates a realistic analytics platform workload to demonstrate
// database migration capabilities. It generates events, executes queries, and provides
// comprehensive telemetry to show the impact of database migrations on live systems.
//
// Key features:
// - Configurable load patterns (events/queries per second)
// - Realistic caching with Redis
// - Full Prometheus metrics integration
// - Self-contained Docker deployment
// - Zero external API dependencies

use anyhow::Result;
use axum::{extract::State, http::StatusCode, response::Response, routing::get, Router};
use clap::Parser;
use prometheus::{Encoder, TextEncoder};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{info, warn};

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
use workers::{EventGeneratorWorker, QuerySimulatorWorker, CacheWarmupWorker};
use crate::workers::SystemMonitorWorker;

/// Shared application state containing all the core services
/// This struct is cloned and passed to all workers and request handlers
#[derive(Clone)]
struct AppState {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured logging with appropriate log levels
    tracing_subscriber::fmt()
        .with_env_filter("analytics_demo=info,sqlx=warn")
        .init();

    // Parse command line arguments and environment variables
    let config = Config::parse();
    info!("Starting analytics demo with config: {:#?}", config);

    // Initialize all core services with connection pooling
    let database = Arc::new(Database::new(&config.database_url).await?);
    let cache = Arc::new(RedisCache::new(&config.redis_url).await?);
    let metrics = Arc::new(AppMetrics::new());
    let generator = Arc::new(DataGenerator::new());

    // Setup database schema and seed with initial test data
    database.setup_schema().await?;
    database.seed_initial_data(&generator, &config).await?;

    // Create shared application state
    let state = AppState {
        database: database.clone(),
        cache: cache.clone(),
        metrics: metrics.clone(),
        generator: generator.clone(),
    };

    // Start background workers that simulate realistic load patterns
    // These run continuously and independently of any external requests
    tokio::spawn(start_event_generator(state.clone(), config.clone()));
    tokio::spawn(start_query_simulator(state.clone(), config.clone()));
    tokio::spawn(start_cache_warmup(state.clone(), config.clone()));
    tokio::spawn(start_system_monitor(state.clone(), config.clone()));

    // Start HTTP server for metrics and health endpoints
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&config.bind_address).await?;
    info!("Metrics server listening on {}", config.bind_address);

    axum::serve(listener, app).await?;
    Ok(())
}

/// HTTP handler that exposes Prometheus metrics
/// This endpoint is scraped by Prometheus to collect all application metrics
async fn metrics_handler(State(state): State<AppState>) -> Result<Response, StatusCode> {
    let encoder = TextEncoder::new();
    let metric_families = state.metrics.registry.gather();

    match encoder.encode_to_string(&metric_families) {
        Ok(output) => Ok(Response::builder()
            .header("content-type", encoder.format_type())
            .body(output.into())
            .unwrap()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Simple health check endpoint for monitoring and load balancer probes
async fn health_handler() -> &'static str {
    "OK"
}

/// Background worker that generates realistic event data
/// Simulates user activity like page views, clicks, and conversions
/// Runs at the configured events_per_second rate
async fn start_event_generator(state: AppState, config: Config) {
    let worker = EventGeneratorWorker::new(state.database, state.cache, state.metrics, state.generator);

    loop {
        if let Err(e) = worker.run_batch(config.events_per_second, config.organizations).await {
            warn!("Event generator error: {}", e);
        }
        // Generate events every second at the configured rate
        sleep(Duration::from_secs(1)).await;
    }
}

/// Background worker that simulates analytics query load
/// Executes dashboard queries, reports, and real-time analytics
/// Implements realistic caching patterns with Redis
async fn start_query_simulator(state: AppState, config: Config) {
    let worker = QuerySimulatorWorker::new(state.database, state.cache, state.metrics);

    loop {
        if let Err(e) = worker.run_batch(config.queries_per_second, config.organizations).await {
            warn!("Query simulator error: {}", e);
        }
        // Execute queries every second at the configured rate
        sleep(Duration::from_secs(1)).await;
    }
}

/// Background worker that maintains cache warmth
/// Pre-loads popular queries into Redis cache to maintain realistic hit ratios
/// Runs every minute to refresh expired cache entries
async fn start_cache_warmup(state: AppState, config: Config) {
    let worker = CacheWarmupWorker::new(state.database, state.cache, state.metrics);

    loop {
        if let Err(e) = worker.warmup_popular_queries(config.organizations).await {
            warn!("Cache warmup error: {}", e);
        }
        // Warmup cache every minute
        sleep(Duration::from_secs(60)).await;
    }
}

async fn start_system_monitor(state: AppState, config: Config) {
    let worker = SystemMonitorWorker::new(state.database, state.metrics);

    loop {
        if let Err(e) = worker.update_system_metrics(&config).await {
            warn!("System monitor error: {}", e);
        }
        sleep(Duration::from_secs(10)).await; // Update every 10 seconds
    }
}