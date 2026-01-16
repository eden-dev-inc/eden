// Background Workers
//
// Re-architected for Redis-only hot path demonstration.
// All cache misses generate synthetic data - no Postgres queries during runtime.
// Postgres is only used for initial seeding, not for live traffic.

use anyhow::Result;
use chrono::{Duration, Utc};
use rand::{rngs::StdRng, SeedableRng, Rng};
use std::{sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration as TokioDuration};
use tracing::{debug, info, error, warn};
use uuid::Uuid;

use crate::{
    database::RedisCache,
    generators::DataGenerator,
    metrics::AppMetrics,
    models::{
        AnalyticsOverview, EventTypeDistribution, HourlyMetrics,
        PagePerformance, TopPage, UserActivity,
    },
    validation::DataValidator,
};
use crate::config::Config;

/// Shared cache of organization IDs - initialized synthetically, no DB needed
pub struct OrgIdCache {
    org_ids: RwLock<Vec<Uuid>>,
    user_ids_by_org: RwLock<std::collections::HashMap<Uuid, Vec<Uuid>>>,
}

impl OrgIdCache {
    pub fn new() -> Self {
        Self {
            org_ids: RwLock::new(Vec::new()),
            user_ids_by_org: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Initialize with synthetic org and user IDs (no DB needed)
    pub async fn initialize_synthetic(&self, num_orgs: u32, users_per_org: u32) {
        let mut org_ids = Vec::with_capacity(num_orgs as usize);
        let mut user_map = std::collections::HashMap::new();

        for _ in 0..num_orgs {
            let org_id = Uuid::new_v4();
            org_ids.push(org_id);

            // Generate synthetic user IDs for this org (cap at 100 for memory)
            let user_ids: Vec<Uuid> = (0..users_per_org.min(100))
                .map(|_| Uuid::new_v4())
                .collect();
            user_map.insert(org_id, user_ids);
        }

        *self.org_ids.write().await = org_ids;
        *self.user_ids_by_org.write().await = user_map;

        info!("Initialized synthetic cache with {} orgs, ~{} users each", num_orgs, users_per_org.min(100));
    }

    pub async fn get_random_org_id(&self) -> Option<Uuid> {
        let org_ids = self.org_ids.read().await;
        if org_ids.is_empty() {
            return None;
        }
        let mut rng = StdRng::from_entropy();
        Some(org_ids[rng.gen_range(0..org_ids.len())])
    }

    pub async fn get_org_ids(&self) -> Vec<Uuid> {
        self.org_ids.read().await.clone()
    }

    pub async fn get_user_ids(&self, org_id: Uuid) -> Vec<Uuid> {
        let map = self.user_ids_by_org.read().await;
        map.get(&org_id).cloned().unwrap_or_default()
    }
}

/// Synthetic data generator for cache population
pub struct SyntheticDataGenerator;

impl SyntheticDataGenerator {
    /// Generate realistic-looking analytics overview
    pub fn analytics_overview(org_id: Uuid, hours: i32) -> AnalyticsOverview {
        let mut rng = StdRng::from_entropy();
        let base_events = rng.gen_range(10000..100000) * (hours as i64) / 24;

        // Add variation to percentages (±20% of base rate)
        let page_view_rate = 0.6 + rng.gen_range(-0.12..0.12);
        let conversion_rate_base = 0.02 + rng.gen_range(-0.008..0.015);

        let page_views = (base_events as f64 * page_view_rate) as i64;
        let conversions = (base_events as f64 * conversion_rate_base) as i64;

        AnalyticsOverview {
            organization_id: org_id,
            total_events: base_events,
            unique_users: base_events / rng.gen_range(5..15),
            page_views,
            conversions,
            conversion_rate: if page_views > 0 { (conversions as f64 / page_views as f64) * 100.0 } else { 0.0 },
            time_period: format!("{}h", hours),
        }
    }

    /// Generate hourly metrics
    pub fn hourly_metrics(org_id: Uuid, hour_offset: i32) -> HourlyMetrics {
        let mut rng = StdRng::from_entropy();
        let hour = Utc::now() - Duration::hours(hour_offset as i64);

        // Simulate realistic daily patterns with gradual peaks
        let hour_of_day = hour.format("%H").to_string().parse::<f64>().unwrap_or(12.0);
        // Bell curve centered at 14:00 (2pm) with morning and evening shoulders
        let traffic_multiplier = 0.5 + 1.2 * (-(hour_of_day - 14.0).powi(2) / 50.0).exp()
            + 0.3 * (-(hour_of_day - 10.0).powi(2) / 20.0).exp()
            + rng.gen_range(-0.15..0.15); // Add noise

        let base = (rng.gen_range(500..2000) as f64 * traffic_multiplier.max(0.3)) as i64;

        // Add variation to event type percentages (±25% of base rate)
        let page_view_rate = 0.6 + rng.gen_range(-0.15..0.15);
        let click_rate = 0.25 + rng.gen_range(-0.06..0.06);
        let conversion_rate = 0.02 + rng.gen_range(-0.008..0.012);
        let signup_rate = 0.005 + rng.gen_range(-0.002..0.003);
        let purchase_rate = 0.003 + rng.gen_range(-0.001..0.002);

        HourlyMetrics {
            organization_id: org_id,
            hour,
            events: base,
            unique_users: base / rng.gen_range(3..8),
            page_views: (base as f64 * page_view_rate) as i64,
            clicks: (base as f64 * click_rate) as i64,
            conversions: (base as f64 * conversion_rate) as i64,
            signups: (base as f64 * signup_rate) as i64,
            purchases: (base as f64 * purchase_rate) as i64,
            revenue: rng.gen_range(100.0..5000.0),
        }
    }

    /// Generate top pages list
    pub fn top_pages() -> Vec<TopPage> {
        let mut rng = StdRng::from_entropy();
        let pages = [
            "/dashboard", "/analytics", "/reports", "/settings",
            "/users", "/billing", "/integrations", "/help",
            "/docs", "/profile",
        ];

        pages.iter().enumerate().map(|(i, &url)| {
            let base_views = rng.gen_range(1000..10000) / (i + 1) as i64;
            TopPage {
                url: format!("https://app.example.com{}", url),
                views: base_views,
                unique_visitors: base_views / rng.gen_range(2..5),
            }
        }).collect()
    }

    /// Generate event type distribution
    pub fn event_distribution(org_id: Uuid) -> EventTypeDistribution {
        let mut rng = StdRng::from_entropy();
        let page_views = rng.gen_range(50000..200000);
        let clicks = rng.gen_range(20000..80000);
        let conversions = rng.gen_range(1000..5000);
        let signups = rng.gen_range(100..1000);
        let purchases = rng.gen_range(50..500);

        EventTypeDistribution {
            organization_id: org_id,
            page_views,
            clicks,
            conversions,
            signups,
            purchases,
            total: page_views + clicks + conversions + signups + purchases,
        }
    }

    /// Generate user activity
    pub fn user_activity(user_id: Uuid, org_id: Uuid) -> UserActivity {
        let mut rng = StdRng::from_entropy();

        UserActivity {
            user_id,
            organization_id: org_id,
            total_events: rng.gen_range(10..500),
            last_seen: Utc::now() - Duration::minutes(rng.gen_range(1..1440)),
            page_views: rng.gen_range(5..200),
            clicks: rng.gen_range(2..100),
            conversions: rng.gen_range(0..10),
            lifetime_value: rng.gen_range(0.0..1000.0),
        }
    }

    /// Generate page performance
    pub fn page_performance(org_id: Uuid, page_url: &str) -> PagePerformance {
        let mut rng = StdRng::from_entropy();
        let views = rng.gen_range(1000..50000);

        PagePerformance {
            organization_id: org_id,
            page_url: page_url.to_string(),
            views,
            unique_visitors: views / rng.gen_range(2..5),
            avg_time_on_page: rng.gen_range(15.0..180.0),
            bounce_rate: rng.gen_range(20.0..70.0),
            conversions: (views as f64 * rng.gen_range(0.01..0.05)) as i64,
        }
    }

    /// Generate realtime stats
    pub fn realtime_stats(org_id: Uuid) -> serde_json::Value {
        let mut rng = StdRng::from_entropy();
        serde_json::json!({
            "organization_id": org_id,
            "current_active_users": rng.gen_range(10..500),
            "events_last_minute": rng.gen_range(50..500),
            "events_last_hour": rng.gen_range(3000..30000)
        })
    }
}

/// QuerySimulatorWorker - Redis-only hot path
/// All cache misses generate synthetic data, no Postgres queries
pub struct QuerySimulatorWorker {
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
    validator: Arc<DataValidator>,
}

impl QuerySimulatorWorker {
    pub fn new(
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
        generator: Arc<DataGenerator>,
        org_cache: Arc<OrgIdCache>,
        validator: Arc<DataValidator>,
    ) -> Self {
        Self {
            cache,
            metrics,
            generator,
            org_cache,
            validator,
        }
    }

    /// Start worker pool - all workers run at maximum speed
    pub async fn start_worker_pool(&self, _queries_per_second: u64, _organizations: u32, max_workers: usize) {
        let num_workers = std::cmp::max(max_workers, 10);

        info!("Starting {} query workers (Redis-only mode, no DB fallback)", num_workers);

        for worker_id in 0..num_workers {
            let cache = self.cache.clone();
            let metrics = self.metrics.clone();
            let generator = self.generator.clone();
            let org_cache = self.org_cache.clone();
            let validator = self.validator.clone();

            tokio::spawn(async move {
                let worker = QuerySimulatorWorker { cache, metrics, generator, org_cache, validator };
                worker.run_worker(worker_id).await;
            });
        }
    }

    async fn run_worker(&self, worker_id: usize) {
        debug!("Query worker {} started (Redis-only)", worker_id);

        loop {
            match self.org_cache.get_random_org_id().await {
                Some(org_id) => {
                    if let Err(e) = self.execute_diverse_query(org_id).await {
                        error!("Worker {} query error: {}", worker_id, e);
                    }
                }
                None => {
                    debug!("Worker {} waiting for org cache", worker_id);
                    sleep(TokioDuration::from_millis(100)).await;
                }
            }
        }
    }

    /// Execute diverse query types with weighted distribution
    async fn execute_diverse_query(&self, org_id: Uuid) -> Result<()> {
        let mut rng = StdRng::from_entropy();
        let query_type = rng.gen_range(0..100);

        let start = Instant::now();
        let result = match query_type {
            0..=39 => self.get_analytics_overview(org_id, 24).await,
            40..=59 => {
                let hour_offset = rng.gen_range(0..24);
                self.get_hourly_metrics(org_id, hour_offset).await
            }
            60..=69 => self.get_top_pages(org_id).await,
            70..=79 => self.get_event_distribution(org_id).await,
            80..=84 => self.get_random_user_activity(org_id).await,
            85..=89 => self.get_random_page_performance(org_id).await,
            90..=94 => self.get_realtime_stats(org_id).await,
            _ => self.get_analytics_overview(org_id, 1).await,
        };
        let latency_ns = start.elapsed().as_nanos() as u64;

        // Record live latency using AtomicU64
        self.metrics.record_live_latency_ns(latency_ns);

        match result {
            Ok(cache_hit) => {
                self.metrics.record_operation_success("analytics_query");
                self.metrics.queries_executed_total.inc();
                if cache_hit {
                    self.metrics.cache_hits_total.inc();
                } else {
                    self.metrics.cache_misses_total.inc();
                }
            }
            Err(e) => {
                self.metrics.record_operation_error("analytics_query", "execution_error");
                error!("Query error for org {}: {}", org_id, e);
            }
        }

        Ok(())
    }

    async fn get_analytics_overview(&self, org_id: Uuid, hours: i32) -> Result<bool> {
        let cache_key = self.generator.cache_key_overview(org_id, hours as u32);

        match self.cache.get::<AnalyticsOverview>(&cache_key, &self.metrics).await {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => {}
            Err(e) => debug!("Cache get error: {}", e),
        }

        // Cache miss - generate synthetic data and cache it with validation
        let data = SyntheticDataGenerator::analytics_overview(org_id, hours);
        if let Err(e) = self.cache.set_and_validate(
            &cache_key, &data, 900, &self.metrics, &self.validator, "analytics_overview"
        ).await {
            debug!("Cache set error: {}", e);
        }

        Ok(false)
    }

    async fn get_hourly_metrics(&self, org_id: Uuid, hour_offset: i32) -> Result<bool> {
        let hour = Utc::now() - Duration::hours(hour_offset as i64);
        let cache_key = self.generator.cache_key_hourly(org_id, hour);

        match self.cache.get::<HourlyMetrics>(&cache_key, &self.metrics).await {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => {}
            Err(e) => debug!("Cache get error: {}", e),
        }

        let data = SyntheticDataGenerator::hourly_metrics(org_id, hour_offset);
        if let Err(e) = self.cache.set_and_validate(
            &cache_key, &data, 3600, &self.metrics, &self.validator, "hourly_metrics"
        ).await {
            debug!("Cache set error: {}", e);
        }

        Ok(false)
    }

    async fn get_top_pages(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = self.generator.cache_key_top_pages(org_id, 24);

        match self.cache.get::<Vec<TopPage>>(&cache_key, &self.metrics).await {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => {}
            Err(e) => debug!("Cache get error: {}", e),
        }

        let data = SyntheticDataGenerator::top_pages();
        if let Err(e) = self.cache.set_and_validate(
            &cache_key, &data, 1200, &self.metrics, &self.validator, "top_pages"
        ).await {
            debug!("Cache set error: {}", e);
        }

        Ok(false)
    }

    async fn get_event_distribution(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = self.generator.cache_key_event_distribution(org_id, "24h");

        match self.cache.get::<EventTypeDistribution>(&cache_key, &self.metrics).await {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => {}
            Err(e) => debug!("Cache get error: {}", e),
        }

        let data = SyntheticDataGenerator::event_distribution(org_id);
        if let Err(e) = self.cache.set_and_validate(
            &cache_key, &data, 900, &self.metrics, &self.validator, "event_distribution"
        ).await {
            debug!("Cache set error: {}", e);
        }

        Ok(false)
    }

    async fn get_random_user_activity(&self, org_id: Uuid) -> Result<bool> {
        let user_ids = self.org_cache.get_user_ids(org_id).await;
        if user_ids.is_empty() {
            return Ok(false);
        }

        let user_id = user_ids[StdRng::from_entropy().gen_range(0..user_ids.len())];
        let cache_key = self.generator.cache_key_user_activity(user_id);

        match self.cache.get::<UserActivity>(&cache_key, &self.metrics).await {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => {}
            Err(e) => debug!("Cache get error: {}", e),
        }

        let data = SyntheticDataGenerator::user_activity(user_id, org_id);
        if let Err(e) = self.cache.set_and_validate(
            &cache_key, &data, 1800, &self.metrics, &self.validator, "user_activity"
        ).await {
            debug!("Cache set error: {}", e);
        }

        Ok(false)
    }

    async fn get_random_page_performance(&self, org_id: Uuid) -> Result<bool> {
        let pages = self.generator.get_popular_pages();
        let page = pages[StdRng::from_entropy().gen_range(0..pages.len())];
        let page_url = format!("https://app.example.com{}", page);
        let cache_key = self.generator.cache_key_page(org_id, &page_url);

        match self.cache.get::<PagePerformance>(&cache_key, &self.metrics).await {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => {}
            Err(e) => debug!("Cache get error: {}", e),
        }

        let data = SyntheticDataGenerator::page_performance(org_id, &page_url);
        if let Err(e) = self.cache.set_and_validate(
            &cache_key, &data, 1800, &self.metrics, &self.validator, "page_performance"
        ).await {
            debug!("Cache set error: {}", e);
        }

        Ok(false)
    }

    async fn get_realtime_stats(&self, org_id: Uuid) -> Result<bool> {
        let cache_key = self.generator.cache_key_realtime(org_id);

        match self.cache.get::<serde_json::Value>(&cache_key, &self.metrics).await {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => {}
            Err(e) => debug!("Cache get error: {}", e),
        }

        // Realtime stats use serde_json::Value, so use regular set
        let data = SyntheticDataGenerator::realtime_stats(org_id);
        if let Err(e) = self.cache.set(&cache_key, &data, 60, &self.metrics).await {
            debug!("Cache set error: {}", e);
        }

        Ok(false)
    }
}

/// CacheWarmupWorker - Pre-populates cache with synthetic data (no DB)
pub struct CacheWarmupWorker {
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
}

impl CacheWarmupWorker {
    pub fn new(
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
        generator: Arc<DataGenerator>,
        org_cache: Arc<OrgIdCache>,
    ) -> Self {
        Self {
            cache,
            metrics,
            generator,
            org_cache,
        }
    }

    /// Bulk populate cache with synthetic data (no DB queries)
    pub async fn bulk_populate(&self) -> Result<()> {
        info!("Starting bulk cache population with synthetic data...");
        let start = Instant::now();
        let mut total_keys = 0u64;

        let org_ids = self.org_cache.get_org_ids().await;
        let org_count = org_ids.len();

        const CHUNK_SIZE: usize = 10;
        for (chunk_idx, org_chunk) in org_ids.chunks(CHUNK_SIZE).enumerate() {
            let mut batch_entries: Vec<(String, String, u64)> = Vec::new();

            for &org_id in org_chunk {
                // Analytics overview for multiple time ranges
                for hours in [1, 6, 24, 168] {
                    let data = SyntheticDataGenerator::analytics_overview(org_id, hours);
                    let key = self.generator.cache_key_overview(org_id, hours as u32);
                    if let Ok(json) = serde_json::to_string(&data) {
                        batch_entries.push((key, json, 900));
                    }
                }

                // Hourly metrics for last 24 hours
                for hour_offset in 0..24 {
                    let data = SyntheticDataGenerator::hourly_metrics(org_id, hour_offset);
                    let hour = Utc::now() - Duration::hours(hour_offset as i64);
                    let key = self.generator.cache_key_hourly(org_id, hour);
                    if let Ok(json) = serde_json::to_string(&data) {
                        batch_entries.push((key, json, 3600));
                    }
                }

                // Top pages
                let data = SyntheticDataGenerator::top_pages();
                let key = self.generator.cache_key_top_pages(org_id, 24);
                if let Ok(json) = serde_json::to_string(&data) {
                    batch_entries.push((key, json, 1200));
                }

                // Event distribution
                let data = SyntheticDataGenerator::event_distribution(org_id);
                let key = self.generator.cache_key_event_distribution(org_id, "24h");
                if let Ok(json) = serde_json::to_string(&data) {
                    batch_entries.push((key, json, 900));
                }

                // Page performance for all popular pages
                for page in self.generator.get_popular_pages() {
                    let page_url = format!("https://app.example.com{}", page);
                    let data = SyntheticDataGenerator::page_performance(org_id, &page_url);
                    let key = self.generator.cache_key_page(org_id, &page_url);
                    if let Ok(json) = serde_json::to_string(&data) {
                        batch_entries.push((key, json, 1800));
                    }
                }

                // User activity for sampled users
                let user_ids = self.org_cache.get_user_ids(org_id).await;
                for user_id in user_ids.iter().take(20) {
                    let data = SyntheticDataGenerator::user_activity(*user_id, org_id);
                    let key = self.generator.cache_key_user_activity(*user_id);
                    if let Ok(json) = serde_json::to_string(&data) {
                        batch_entries.push((key, json, 1800));
                    }
                }

                // Realtime counters
                let data = SyntheticDataGenerator::realtime_stats(org_id);
                let key = self.generator.cache_key_realtime(org_id);
                if let Ok(json) = serde_json::to_string(&data) {
                    batch_entries.push((key, json, 60));
                }

                // Rolling window metrics
                for minutes in [5, 15, 30, 60] {
                    let key = self.generator.cache_key_rolling_window(org_id, "events", minutes);
                    let data = serde_json::json!({"count": StdRng::from_entropy().gen_range(100..10000), "window_minutes": minutes});
                    if let Ok(json) = serde_json::to_string(&data) {
                        batch_entries.push((key, json, (minutes * 60) as u64));
                    }
                }
            }

            let chunk_size = batch_entries.len();
            if !batch_entries.is_empty() {
                if let Err(e) = self.cache.set_batch_json(batch_entries, &self.metrics).await {
                    error!("Batch cache write failed for chunk {}: {}", chunk_idx, e);
                } else {
                    total_keys += chunk_size as u64;
                    debug!("Populated chunk {} with {} keys", chunk_idx, chunk_size);
                }
            }
        }

        let duration = start.elapsed().as_secs_f64();
        info!(
            "Bulk cache population completed: {} keys for {} orgs in {:.2}s ({:.0} keys/sec)",
            total_keys, org_count, duration, total_keys as f64 / duration
        );

        Ok(())
    }

    /// Periodic refresh with synthetic data (no DB)
    pub async fn warmup_refresh(&self) -> Result<()> {
        debug!("Running cache warmup refresh cycle");
        let start = Instant::now();
        let mut refreshed_count = 0u64;

        let org_ids = self.org_cache.get_org_ids().await;
        let mut batch_entries: Vec<(String, String, u64)> = Vec::new();

        for org_id in org_ids {
            // Refresh overview for multiple time ranges
            for hours in [1, 6, 24] {
                let data = SyntheticDataGenerator::analytics_overview(org_id, hours);
                let key = self.generator.cache_key_overview(org_id, hours as u32);
                if let Ok(json) = serde_json::to_string(&data) {
                    batch_entries.push((key, json, 900));
                    refreshed_count += 1;
                }
            }

            // Refresh recent hourly metrics (last 6 hours)
            for hour_offset in 0..6 {
                let data = SyntheticDataGenerator::hourly_metrics(org_id, hour_offset);
                let hour = Utc::now() - Duration::hours(hour_offset as i64);
                let key = self.generator.cache_key_hourly(org_id, hour);
                if let Ok(json) = serde_json::to_string(&data) {
                    batch_entries.push((key, json, 3600));
                    refreshed_count += 1;
                }
            }

            // Batch write every 100 entries
            if batch_entries.len() >= 100 {
                if let Err(e) = self.cache.set_batch_json(batch_entries.clone(), &self.metrics).await {
                    error!("Batch cache write failed: {}", e);
                }
                batch_entries.clear();
            }
        }

        if !batch_entries.is_empty() {
            if let Err(e) = self.cache.set_batch_json(batch_entries, &self.metrics).await {
                error!("Final batch cache write failed: {}", e);
            }
        }

        let duration = start.elapsed().as_secs_f64();
        debug!("Cache warmup completed: {} entries in {:.2}s", refreshed_count, duration);

        Ok(())
    }
}

/// EventSimulatorWorker - Simulates event traffic via Redis INCR operations only
/// No database writes - purely Redis operations
pub struct EventSimulatorWorker {
    cache: Arc<RedisCache>,
    metrics: Arc<AppMetrics>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
}

impl EventSimulatorWorker {
    pub fn new(
        cache: Arc<RedisCache>,
        metrics: Arc<AppMetrics>,
        generator: Arc<DataGenerator>,
        org_cache: Arc<OrgIdCache>,
    ) -> Self {
        Self {
            cache,
            metrics,
            generator,
            org_cache,
        }
    }

    /// Simulate events by incrementing Redis counters (no DB writes)
    pub async fn run_batch(&self, events_per_second: u64) -> Result<()> {
        let start = Instant::now();
        let org_ids = self.org_cache.get_org_ids().await;

        if org_ids.is_empty() {
            warn!("No organizations available for event simulation");
            return Ok(());
        }

        // Batch increment counters for simulated events
        let mut counter_keys: Vec<String> = Vec::with_capacity(events_per_second as usize);
        let mut rng = StdRng::from_entropy();

        for _ in 0..events_per_second {
            let org_id = org_ids[rng.gen_range(0..org_ids.len())];
            counter_keys.push(self.generator.cache_key_realtime_counter(org_id, "minute"));

            // Record event metrics
            let event_types = ["page_view", "click", "conversion", "sign_up", "purchase"];
            let weights = [60, 28, 8, 3, 1];
            let total_weight: i32 = weights.iter().sum();
            let mut roll = rng.gen_range(0..total_weight);
            let mut selected_type = event_types[0];
            for (i, &weight) in weights.iter().enumerate() {
                if roll < weight {
                    selected_type = event_types[i];
                    break;
                }
                roll -= weight;
            }
            self.metrics.record_event_generated(selected_type);
        }

        // Batch increment all counters via Redis pipeline
        if let Err(e) = self.cache.incr_batch(&counter_keys, &self.metrics).await {
            error!("Batch counter increment failed: {}", e);
        }

        let duration = start.elapsed().as_secs_f64();
        self.metrics.event_generation_duration.observe(duration);

        debug!("Simulated {} events in {:.2}ms", events_per_second, duration * 1000.0);
        Ok(())
    }
}

/// SystemMonitorWorker - Updates system metrics (no DB dependency)
pub struct SystemMonitorWorker {
    metrics: Arc<AppMetrics>,
    org_cache: Arc<OrgIdCache>,
}

impl SystemMonitorWorker {
    pub fn new(metrics: Arc<AppMetrics>, org_cache: Arc<OrgIdCache>) -> Self {
        Self { metrics, org_cache }
    }

    pub async fn update_system_metrics(&self, config: &Config) -> Result<()> {
        // No active DB connections in Redis-only mode
        self.metrics.db_connections_active.set(0);

        let org_count = self.org_cache.get_org_ids().await.len() as i64;
        self.metrics.active_organizations.set(org_count);
        self.metrics.events_per_second.set(config.events_per_second as i64);

        // Log live latency stats
        self.metrics.log_live_latency();

        // Log live validation stats
        self.metrics.log_live_validation();

        Ok(())
    }
}