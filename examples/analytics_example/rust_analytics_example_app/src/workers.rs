// Background Workers
//
// This module contains the background workers that generate realistic load patterns.
// Workers use continuous task pools to achieve high throughput without spawning overhead.

use anyhow::Result;
use rand::{rngs::StdRng, SeedableRng, Rng};
use std::{sync::Arc, time::Instant};
use tokio::time::{sleep, Duration};
use tracing::{debug, info};
use uuid::Uuid;

use crate::{
    database::{Database, RedisCache},
    generators::DataGenerator,
    metrics::AppMetrics,
    models::EventType,
};
use crate::config::Config;
use crate::models::Event;

/// EventGeneratorWorker simulates realistic user activity by creating events
pub struct EventGeneratorWorker {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
}

impl EventGeneratorWorker {
    pub fn new(
        database: Arc<Database>,
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
        generator: Arc<DataGenerator>,
    ) -> Self {
        Self {
            database,
            cache,
            metrics,
            generator,
        }
    }

    pub async fn run_batch(&self, events_per_second: u64, organizations: u32) -> Result<()> {
        let start = Instant::now();
        let mut success_count = 0u64;
        let mut error_count = 0u64;

        let org_ids = self.database.get_random_organization_ids(organizations).await?;
        if org_ids.is_empty() {
            return Ok(());
        }

        let mut org_users: std::collections::HashMap<Uuid, Vec<Uuid>> = std::collections::HashMap::new();
        for &org_id in &org_ids {
            match self.database.get_random_user_ids(org_id, 50).await {
                Ok(users) => { org_users.insert(org_id, users); }
                Err(_) => { org_users.insert(org_id, Vec::new()); }
            }
        }

        const BATCH_SIZE: usize = 500;
        let total_events = events_per_second as usize;
        let num_batches = (total_events + BATCH_SIZE - 1) / BATCH_SIZE;

        for batch_idx in 0..num_batches {
            let batch_start = batch_idx * BATCH_SIZE;
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, total_events);
            let batch_size = batch_end - batch_start;

            let mut events = Vec::with_capacity(batch_size);
            let mut event_types = Vec::with_capacity(batch_size);

            for _ in 0..batch_size {
                let org_id = org_ids[StdRng::from_entropy().gen_range(0..org_ids.len())];
                let user_ids = org_users.get(&org_id).map(|v| v.as_slice()).unwrap_or(&[]);

                let event = self.generator.generate_event(org_id, user_ids);
                event_types.push(event.event_type.clone());
                events.push(event);
            }

            let insert_start = Instant::now();
            match self.database.insert_events_batch(&events).await {
                Ok(rows_affected) => {
                    let actual_insert_duration = insert_start.elapsed().as_secs_f64();
                    success_count += rows_affected;

                    self.metrics.record_operation_success("event_generation");
                    self.metrics.record_db_operation("batch_insert", "success", actual_insert_duration);

                    self.handle_cache_invalidation(&events).await?;

                    for event_type in &event_types {
                        self.metrics.record_event_generated(event_type);
                    }
                }
                Err(e) => {
                    let actual_insert_duration = insert_start.elapsed().as_secs_f64();
                    error_count += batch_size as u64;

                    self.metrics.record_operation_error("event_generation", "batch_insert_error");
                    self.metrics.record_db_operation("batch_insert", "error", actual_insert_duration);

                    tracing::warn!("Batch insert failed for {} events: {}", batch_size, e);
                }
            }
        }

        let total_duration = start.elapsed().as_secs_f64();
        self.metrics.event_generation_duration.observe(total_duration);

        debug!(
            "Generated {} events ({} success, {} errors) in {:.2}s",
            events_per_second, success_count, error_count, total_duration
        );

        Ok(())
    }

    async fn handle_cache_invalidation(&self, events: &[Event]) -> Result<()> {
        let mut orgs_to_invalidate = std::collections::HashSet::new();

        for event in events {
            match EventType::from_str(&event.event_type) {
                Some(EventType::Purchase) | Some(EventType::Conversion) | Some(EventType::SignUp) => {
                    orgs_to_invalidate.insert(event.organization_id);
                }
                _ => {}
            }
        }

        for org_id in orgs_to_invalidate {
            let cache_pattern = format!("analytics:{}:*", org_id);
            let cache_start = Instant::now();

            match self.cache.invalidate_pattern(&cache_pattern, &self.metrics).await {
                Ok(_) => {
                    self.metrics.record_cache_operation("invalidate", "success", cache_start.elapsed().as_secs_f64());
                }
                Err(e) => {
                    self.metrics.record_cache_operation("invalidate", "error", cache_start.elapsed().as_secs_f64());
                    tracing::warn!("Cache invalidation failed for org {}: {}", org_id, e);
                }
            }
        }

        Ok(())
    }
}

/// QuerySimulatorWorker executes analytics queries with proper cache-first pattern
/// Uses a worker pool approach to achieve high query throughput
pub struct QuerySimulatorWorker {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
}

impl QuerySimulatorWorker {
    pub fn new(
        database: Arc<Database>,
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
    ) -> Self {
        Self {
            database,
            cache,
            metrics,
        }
    }

    /// Spawn persistent worker pool that continuously executes queries
    /// Each worker runs independently to achieve target throughput
    pub async fn start_worker_pool(&self, queries_per_second: u64, organizations: u32, num_workers: usize) {
        info!("Starting query worker pool with {} workers for {} qps", num_workers, queries_per_second);

        // Calculate delay between queries per worker to achieve target QPS
        let queries_per_worker = queries_per_second as f64 / num_workers as f64;
        let delay_micros = if queries_per_worker > 0.0 {
            (1_000_000.0 / queries_per_worker) as u64
        } else {
            100_000
        };

        info!("Each worker will execute with {}µs delay between queries", delay_micros);

        for worker_id in 0..num_workers {
            let database = self.database.clone();
            let cache = self.cache.clone();
            let metrics = self.metrics.clone();

            tokio::spawn(async move {
                let worker = QuerySimulatorWorker { database, cache, metrics };
                worker.run_worker(worker_id, delay_micros, organizations).await;
            });
        }
    }

    /// Individual worker that continuously executes queries
    async fn run_worker(&self, worker_id: usize, delay_micros: u64, organizations: u32) {
        info!("Query worker {} started with {}µs delay", worker_id, delay_micros);

        loop {
            // Get random org
            match self.database.get_random_organization_ids(organizations).await {
                Ok(org_ids) if !org_ids.is_empty() => {
                    let org_id = org_ids[StdRng::from_entropy().gen_range(0..org_ids.len())];

                    if let Err(e) = self.execute_random_query(org_id).await {
                        tracing::warn!("Worker {} query error: {}", worker_id, e);
                    }
                }
                _ => {
                    tracing::warn!("Worker {} could not get org IDs", worker_id);
                }
            }

            sleep(Duration::from_micros(delay_micros)).await;
        }
    }

    async fn execute_random_query(&self, org_id: Uuid) -> Result<()> {
        let mut rng = StdRng::from_entropy();
        let query_type = rng.gen_range(0..100);

        let result = match query_type {
            0..=69 => self.get_analytics_overview(org_id).await,
            70..=89 => self.get_top_pages(org_id).await,
            _ => self.get_hourly_stats(org_id).await,
        };

        match result {
            Ok(cache_hit) => {
                self.metrics.record_operation_success("analytics_query");
                self.metrics.queries_executed_total.inc();
                // Record cache hit/miss for metrics
                if cache_hit {
                    self.metrics.cache_hits_total.inc();
                } else {
                    self.metrics.cache_misses_total.inc();
                }
            }
            Err(e) => {
                self.metrics.record_operation_error("analytics_query", "execution_error");
                tracing::warn!("Query execution error: {}", e);
            }
        }

        Ok(())
    }

    async fn get_analytics_overview(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = format!("analytics:{}:overview:24h", org_id);

        // Try cache first
        let cache_result = self.cache.get::<serde_json::Value>(&cache_key, &self.metrics).await;

        match cache_result {
            Ok(Some(_data)) => {
                return Ok(true); // Cache hit
            }
            Ok(None) => {
                // Cache miss - proceed to DB query
            }
            Err(e) => {
                tracing::warn!("Cache get error for overview: {}", e);
            }
        }

        // Cache miss - query database
        let db_start = Instant::now();
        let overview = self.database.get_analytics_overview(org_id, 24).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        // Populate cache
        if let Err(e) = self.cache.set(&cache_key, &overview, 300, &self.metrics).await {
            tracing::warn!("Failed to set cache for overview: {}", e);
        }

        Ok(false)
    }

    async fn get_top_pages(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = format!("analytics:{}:top_pages:24h", org_id);

        let cache_result = self.cache.get::<Vec<serde_json::Value>>(&cache_key, &self.metrics).await;

        match cache_result {
            Ok(Some(_data)) => {
                return Ok(true);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("Cache get error for top pages: {}", e);
            }
        }

        let db_start = Instant::now();
        let top_pages = self.database.get_top_pages(org_id, 10).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        if let Err(e) = self.cache.set(&cache_key, &top_pages, 600, &self.metrics).await {
            tracing::warn!("Failed to set cache for top pages: {}", e);
        }

        Ok(false)
    }

    async fn get_hourly_stats(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = format!("analytics:{}:hourly:1h", org_id);

        let cache_result = self.cache.get::<serde_json::Value>(&cache_key, &self.metrics).await;

        match cache_result {
            Ok(Some(_data)) => {
                return Ok(true);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("Cache get error for hourly stats: {}", e);
            }
        }

        let db_start = Instant::now();
        let stats = self.database.get_analytics_overview(org_id, 1).await?;
        let db_duration = db_start.elapsed().as_secs_f64();
        self.metrics.record_db_operation("select", "success", db_duration);

        if let Err(e) = self.cache.set(&cache_key, &stats, 180, &self.metrics).await {
            tracing::warn!("Failed to set cache for hourly stats: {}", e);
        }

        Ok(false)
    }
}

/// CacheWarmupWorker proactively refreshes cache on intervals
pub struct CacheWarmupWorker {
    database: Arc<Database>,
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
}

impl CacheWarmupWorker {
    pub fn new(
        database: Arc<Database>,
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
    ) -> Self {
        Self {
            database,
            cache,
            metrics,
        }
    }

    pub async fn warmup_popular_queries(&self, organizations: u32) -> Result<()> {
        info!("Starting cache warmup");

        let org_ids = self.database.get_random_organization_ids(organizations).await?;

        let mut refreshed_count = 0;

        for org_id in org_ids {
            // Refresh all three query types
            for (key_suffix, hours, ttl) in [
                ("overview:24h", 24, 300),
                ("top_pages:24h", 24, 600),
                ("hourly:1h", 1, 180),
            ] {
                let cache_key = format!("analytics:{}:{}", org_id, key_suffix);

                let db_start = Instant::now();
                match self.database.get_analytics_overview(org_id, hours).await {
                    Ok(data) => {
                        let db_duration = db_start.elapsed().as_secs_f64();
                        self.metrics.record_db_operation("select", "success", db_duration);

                        if self.cache.set(&cache_key, &data, ttl, &self.metrics).await.is_ok() {
                            refreshed_count += 1;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Warmup query failed for {}: {}", cache_key, e);
                    }
                }
            }

            // Also warmup top pages specifically
            let pages_key = format!("analytics:{}:top_pages:24h", org_id);
            let db_start = Instant::now();
            match self.database.get_top_pages(org_id, 10).await {
                Ok(pages) => {
                    let db_duration = db_start.elapsed().as_secs_f64();
                    self.metrics.record_db_operation("select", "success", db_duration);

                    if self.cache.set(&pages_key, &pages, 600, &self.metrics).await.is_ok() {
                        refreshed_count += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!("Warmup top pages failed for org {}: {}", org_id, e);
                }
            }
        }

        info!("Cache warmup completed: {} entries refreshed", refreshed_count);
        Ok(())
    }
}

pub struct SystemMonitorWorker {
    database: Arc<Database>,
    metrics: Arc<AppMetrics>,
}

impl SystemMonitorWorker {
    pub fn new(database: Arc<Database>, metrics: Arc<AppMetrics>) -> Self {
        Self { database, metrics }
    }

    pub async fn update_system_metrics(&self, config: &Config) -> Result<()> {
        let estimated_connections = std::cmp::min(
            (config.queries_per_second + config.events_per_second) / 10,
            20
        ) as i64;

        self.metrics.db_connections_active.set(estimated_connections);

        let org_count = self.database.get_random_organization_ids(1000).await?.len() as i64;
        self.metrics.active_organizations.set(org_count);

        Ok(())
    }
}