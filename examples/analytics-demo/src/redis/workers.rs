// Redis Background Workers
//
// Redis-only hot path workers. All cache misses generate synthetic data.
// No Postgres queries during runtime.

use anyhow::Result;
use chrono::{Duration, Utc};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{sync::Arc, time::Instant};
use tokio::time::{sleep, Duration as TokioDuration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    generators::DataGenerator,
    models::{
        AnalyticsOverview, CohortAnalysis, CohortBreakdown, CommerceSnapshot, DeviceBreakdown,
        DeviceBrowserStats, EventTypeDistribution, FunnelAnalysis, FunnelStep, GeoBreakdown,
        GeographicDistribution, HourlyMetrics, MarketingSnapshot, PagePerformance, PageViewRecord,
        ReferrerBreakdown, ReferrerStats, Session, SessionSnapshot, TopPage, UserActivity,
    },
    redis::RedisCache,
    runtime_controls::RuntimeControls,
    telemetry::{CacheWarmupSummary, EventBatchSummary, TelemetryRuntime, TelemetrySample},
    workers::OrgIdCache,
};

struct QueryExecution {
    query_type: &'static str,
    cache_hit: bool,
    payload: serde_json::Value,
}

fn push_json_entry<T: serde::Serialize>(
    entries: &mut Vec<(String, String, u64)>,
    key: String,
    value: &T,
    ttl_seconds: u64,
) {
    if let Ok(json) = serde_json::to_string(value) {
        entries.push((key, json, ttl_seconds));
    }
}

fn random_user_id(user_ids: &[Uuid], rng: &mut StdRng) -> Option<Uuid> {
    if user_ids.is_empty() {
        None
    } else {
        Some(user_ids[rng.gen_range(0..user_ids.len())])
    }
}

/// Synthetic data generator for cache population
pub struct SyntheticDataGenerator;

impl SyntheticDataGenerator {
    /// Generate realistic-looking analytics overview
    pub fn analytics_overview(org_id: Uuid, hours: i32) -> AnalyticsOverview {
        let mut rng = StdRng::from_entropy();
        let base_events = rng.gen_range(10000..100000) * (hours as i64) / 24;

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
            conversion_rate: if page_views > 0 {
                (conversions as f64 / page_views as f64) * 100.0
            } else {
                0.0
            },
            time_period: format!("{}h", hours),
        }
    }

    /// Generate hourly metrics
    pub fn hourly_metrics(org_id: Uuid, hour_offset: i32) -> HourlyMetrics {
        let mut rng = StdRng::from_entropy();
        let hour = Utc::now() - Duration::hours(hour_offset as i64);

        let hour_of_day = hour.format("%H").to_string().parse::<f64>().unwrap_or(12.0);
        let traffic_multiplier = 0.5
            + 1.2 * (-(hour_of_day - 14.0).powi(2) / 50.0).exp()
            + 0.3 * (-(hour_of_day - 10.0).powi(2) / 20.0).exp()
            + rng.gen_range(-0.15..0.15);

        let base = (rng.gen_range(500..2000) as f64 * traffic_multiplier.max(0.3)) as i64;

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
            "/dashboard",
            "/analytics",
            "/reports",
            "/settings",
            "/users",
            "/billing",
            "/integrations",
            "/help",
            "/docs",
            "/profile",
        ];

        pages
            .iter()
            .enumerate()
            .map(|(i, &url)| {
                let base_views = rng.gen_range(1000..10000) / (i + 1) as i64;
                TopPage {
                    url: format!("https://app.example.com{}", url),
                    views: base_views,
                    unique_visitors: base_views / rng.gen_range(2..5),
                }
            })
            .collect()
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
        json!({
            "organization_id": org_id,
            "current_active_users": rng.gen_range(10..500),
            "events_last_minute": rng.gen_range(50..500),
            "events_last_hour": rng.gen_range(3000..30000)
        })
    }

    pub fn referrer_breakdown(org_id: Uuid) -> ReferrerBreakdown {
        let mut rng = StdRng::from_entropy();
        let sources = [
            "google",
            "organic_search",
            "newsletter",
            "linkedin",
            "partner_referral",
            "direct",
        ]
        .into_iter()
        .map(|source| {
            let visits = rng.gen_range(500..6000);
            let conversions = rng.gen_range(10..400);
            ReferrerStats {
                referrer: source.to_string(),
                visits,
                unique_visitors: visits / rng.gen_range(1..3),
                conversions,
                conversion_rate: conversions as f64 / visits as f64 * 100.0,
            }
        })
        .collect();

        ReferrerBreakdown {
            organization_id: org_id,
            period: "24h".to_string(),
            sources,
        }
    }

    pub fn cohort_breakdown(org_id: Uuid) -> CohortBreakdown {
        let mut rng = StdRng::from_entropy();
        let cohorts = (0..6)
            .map(|month_offset| {
                let cohort_date = Utc::now() - Duration::days(30 * month_offset as i64);
                CohortAnalysis {
                    organization_id: org_id,
                    cohort_period: cohort_date.format("%Y-%m").to_string(),
                    users: rng.gen_range(100..5000),
                    retention_rate: rng.gen_range(35.0..92.0),
                    avg_events_per_user: rng.gen_range(4.0..31.0),
                }
            })
            .collect();

        CohortBreakdown {
            organization_id: org_id,
            cohorts,
        }
    }

    pub fn device_breakdown(org_id: Uuid) -> DeviceBreakdown {
        let mut rng = StdRng::from_entropy();
        let combinations = [
            ("desktop", "Chrome"),
            ("desktop", "Safari"),
            ("mobile", "Safari"),
            ("mobile", "Chrome"),
            ("tablet", "Safari"),
            ("desktop", "Firefox"),
        ];
        let stats = combinations
            .into_iter()
            .map(|(device, browser)| {
                let count = rng.gen_range(50..2500);
                DeviceBrowserStats {
                    organization_id: org_id,
                    device_type: device.to_string(),
                    browser: browser.to_string(),
                    count,
                    percentage: rng.gen_range(1.0..35.0),
                }
            })
            .collect();

        DeviceBreakdown {
            organization_id: org_id,
            period: "24h".to_string(),
            stats,
        }
    }

    pub fn geo_breakdown(org_id: Uuid) -> GeoBreakdown {
        let mut rng = StdRng::from_entropy();
        let regions = [
            ("US", "New York"),
            ("US", "San Francisco"),
            ("GB", "London"),
            ("DE", "Berlin"),
            ("IN", "Bengaluru"),
            ("BR", "Sao Paulo"),
        ]
        .into_iter()
        .map(|(country, city)| GeographicDistribution {
            organization_id: org_id,
            country_code: country.to_string(),
            city: city.to_string(),
            users: rng.gen_range(50..2000),
            events: rng.gen_range(200..12000),
        })
        .collect();

        GeoBreakdown {
            organization_id: org_id,
            period: "24h".to_string(),
            regions,
        }
    }

    pub fn funnel_analysis(org_id: Uuid, funnel_id: &str) -> FunnelAnalysis {
        let mut rng = StdRng::from_entropy();
        let base = rng.gen_range(1000..15000) as f64;
        let steps = [
            "landing_page_view",
            "signup_started",
            "workspace_created",
            "source_connected",
            "first_dashboard_loaded",
        ];
        let mut current_users = base;
        let mut output = Vec::with_capacity(steps.len());

        for step in steps {
            let next_users = (current_users * rng.gen_range(0.55..0.92)).round();
            output.push(FunnelStep {
                step_name: step.to_string(),
                users: current_users.round() as i64,
                conversion_rate: if base > 0.0 {
                    current_users / base * 100.0
                } else {
                    0.0
                },
            });
            current_users = next_users.max(1.0);
        }

        FunnelAnalysis {
            organization_id: org_id,
            funnel_id: funnel_id.to_string(),
            steps: output,
        }
    }

    pub fn session_snapshot(
        generator: &DataGenerator,
        org_id: Uuid,
        user_ids: &[Uuid],
    ) -> SessionSnapshot {
        let mut rng = StdRng::from_entropy();
        let session_count = rng.gen_range(4..10);
        let mut sessions: Vec<Session> = Vec::with_capacity(session_count);
        let mut page_views: Vec<PageViewRecord> = Vec::with_capacity(session_count * 2);

        for _ in 0..session_count {
            let user_id = random_user_id(user_ids, &mut rng);
            let session = generator.generate_session(org_id, user_id);
            let session_id = session.id;
            sessions.push(session);
            page_views.push(generator.generate_page_view_record(org_id, Some(session_id), user_id));
            page_views.push(generator.generate_page_view_record(org_id, Some(session_id), user_id));
        }

        SessionSnapshot {
            organization_id: org_id,
            active_users: sessions.iter().filter(|s| s.user_id.is_some()).count() as i64,
            sessions,
            recent_page_views: page_views,
        }
    }

    pub fn marketing_snapshot(generator: &DataGenerator, org_id: Uuid) -> MarketingSnapshot {
        let campaigns = (0..4)
            .map(|_| generator.generate_campaign(org_id))
            .collect::<Vec<_>>();
        let experiments = (0..3)
            .map(|_| generator.generate_experiment(org_id))
            .collect::<Vec<_>>();
        let goals = (0..4)
            .map(|_| generator.generate_goal(org_id))
            .collect::<Vec<_>>();

        let active_campaigns = campaigns.iter().filter(|c| c.status == "active").count() as i64;
        let running_experiments =
            experiments.iter().filter(|e| e.status == "running").count() as i64;

        MarketingSnapshot {
            organization_id: org_id,
            active_campaigns,
            running_experiments,
            campaigns,
            experiments,
            goals,
        }
    }

    pub fn commerce_snapshot(
        generator: &DataGenerator,
        org_id: Uuid,
        user_ids: &[Uuid],
    ) -> CommerceSnapshot {
        let mut rng = StdRng::from_entropy();
        let plans = (0..3)
            .map(|_| generator.generate_subscription_plan(org_id))
            .collect::<Vec<_>>();
        let products = (0..5)
            .map(|_| generator.generate_product(org_id, None))
            .collect::<Vec<_>>();

        let orders = (0..4)
            .map(|_| {
                let user_id = random_user_id(user_ids, &mut rng).unwrap_or_else(Uuid::new_v4);
                generator.generate_order(org_id, user_id)
            })
            .collect::<Vec<_>>();

        let reviews = (0..4)
            .map(|_| {
                let user_id = random_user_id(user_ids, &mut rng).unwrap_or_else(Uuid::new_v4);
                let product_id = products[rng.gen_range(0..products.len())].id;
                generator.generate_review(org_id, product_id, user_id)
            })
            .collect::<Vec<_>>();

        let subscriptions = (0..4)
            .map(|_| {
                let user_id = random_user_id(user_ids, &mut rng).unwrap_or_else(Uuid::new_v4);
                let plan_id = plans[rng.gen_range(0..plans.len())].id;
                generator.generate_subscription(org_id, user_id, plan_id)
            })
            .collect::<Vec<_>>();

        let invoices = (0..4)
            .map(|_| {
                let user_id = random_user_id(user_ids, &mut rng).unwrap_or_else(Uuid::new_v4);
                generator.generate_invoice(org_id, user_id)
            })
            .collect::<Vec<_>>();

        let payments = (0..4)
            .map(|_| {
                let user_id = random_user_id(user_ids, &mut rng).unwrap_or_else(Uuid::new_v4);
                generator.generate_payment(org_id, user_id)
            })
            .collect::<Vec<_>>();

        let revenue_cents = orders.iter().map(|order| order.total_cents).sum();
        let failed_payments = payments
            .iter()
            .filter(|payment| payment.status == "failed" || payment.status == "disputed")
            .count();
        let payment_failure_rate = if payments.is_empty() {
            0.0
        } else {
            failed_payments as f64 / payments.len() as f64 * 100.0
        };
        let active_subscriptions = subscriptions
            .iter()
            .filter(|subscription| {
                subscription.status == "active" || subscription.status == "trialing"
            })
            .count() as i64;

        CommerceSnapshot {
            organization_id: org_id,
            revenue_cents,
            payment_failure_rate,
            active_subscriptions,
            plans,
            products,
            orders,
            reviews,
            subscriptions,
            invoices,
            payments,
        }
    }

    pub fn event_batch_samples(
        generator: &DataGenerator,
        org_id: Uuid,
        user_ids: &[Uuid],
        sample_size: usize,
    ) -> Vec<TelemetrySample> {
        let mut rng = StdRng::from_entropy();
        let mut samples = Vec::with_capacity(sample_size);

        for _ in 0..sample_size {
            let user_id = random_user_id(user_ids, &mut rng).unwrap_or_else(Uuid::new_v4);
            let (record_type, payload) = match rng.gen_range(0..15) {
                0 => (
                    "session",
                    serde_json::to_value(generator.generate_session(org_id, Some(user_id))),
                ),
                1 => (
                    "page_view_record",
                    serde_json::to_value(generator.generate_page_view_record(
                        org_id,
                        Some(Uuid::new_v4()),
                        Some(user_id),
                    )),
                ),
                2 => (
                    "campaign",
                    serde_json::to_value(generator.generate_campaign(org_id)),
                ),
                3 => (
                    "experiment",
                    serde_json::to_value(generator.generate_experiment(org_id)),
                ),
                4 => (
                    "goal",
                    serde_json::to_value(generator.generate_goal(org_id)),
                ),
                5 => (
                    "product",
                    serde_json::to_value(generator.generate_product(org_id, None)),
                ),
                6 => (
                    "order",
                    serde_json::to_value(generator.generate_order(org_id, user_id)),
                ),
                7 => (
                    "subscription",
                    serde_json::to_value(generator.generate_subscription(
                        org_id,
                        user_id,
                        generator.generate_subscription_plan(org_id).id,
                    )),
                ),
                8 => (
                    "invoice",
                    serde_json::to_value(generator.generate_invoice(org_id, user_id)),
                ),
                9 => (
                    "payment",
                    serde_json::to_value(generator.generate_payment(org_id, user_id)),
                ),
                10 => (
                    "review",
                    serde_json::to_value(generator.generate_review(
                        org_id,
                        generator.generate_product(org_id, None).id,
                        user_id,
                    )),
                ),
                11 => (
                    "referrer_breakdown",
                    serde_json::to_value(SyntheticDataGenerator::referrer_breakdown(org_id)),
                ),
                12 => (
                    "cohort_breakdown",
                    serde_json::to_value(SyntheticDataGenerator::cohort_breakdown(org_id)),
                ),
                13 => (
                    "device_breakdown",
                    serde_json::to_value(SyntheticDataGenerator::device_breakdown(org_id)),
                ),
                _ => (
                    "geo_breakdown",
                    serde_json::to_value(SyntheticDataGenerator::geo_breakdown(org_id)),
                ),
            };

            if let Ok(payload) = payload {
                samples.push(TelemetrySample::new(record_type, payload));
            }
        }

        samples
    }
}

/// QuerySimulatorWorker - Redis-only hot path
/// All cache misses generate synthetic data, no Postgres queries
pub struct QuerySimulatorWorker {
    cache: Arc<RedisCache>,
    telemetry: Arc<TelemetryRuntime>,
    generator: Arc<DataGenerator>,
    controls: Arc<RuntimeControls>,
    org_cache: Arc<OrgIdCache>,
}

impl QuerySimulatorWorker {
    pub fn new(
        cache: Arc<RedisCache>,
        telemetry: Arc<TelemetryRuntime>,
        generator: Arc<DataGenerator>,
        controls: Arc<RuntimeControls>,
        org_cache: Arc<OrgIdCache>,
    ) -> Self {
        Self {
            cache,
            telemetry,
            generator,
            controls,
            org_cache,
        }
    }

    /// Start worker pool - all workers run at maximum speed
    pub async fn start_worker_pool(
        &self,
        _organizations: u32,
        max_workers: usize,
        shutdown: CancellationToken,
    ) {
        let num_workers = std::cmp::max(max_workers, 10);

        info!(
            "Starting {} query workers (Redis-only mode, no DB fallback)",
            num_workers
        );

        for worker_id in 0..num_workers {
            let cache = self.cache.clone();
            let telemetry = self.telemetry.clone();
            let generator = self.generator.clone();
            let controls = self.controls.clone();
            let org_cache = self.org_cache.clone();
            let worker_shutdown = shutdown.clone();

            tokio::spawn(async move {
                let worker = QuerySimulatorWorker {
                    cache,
                    telemetry,
                    generator,
                    controls,
                    org_cache,
                };
                worker.run_worker(worker_id, worker_shutdown).await;
            });
        }
    }

    async fn run_worker(&self, worker_id: usize, shutdown: CancellationToken) {
        debug!("Query worker {} started (Redis-only)", worker_id);

        loop {
            if !self.controls.query_limiter().acquire_until(&shutdown).await {
                break;
            }
            match self.org_cache.get_random_org_id().await {
                Some(org_id) => {
                    if let Err(e) = self.execute_diverse_query(org_id).await {
                        error!("Worker {} query error: {}", worker_id, e);
                    }
                }
                None => {
                    debug!("Worker {} waiting for org cache", worker_id);
                    tokio::select! {
                        _ = sleep(TokioDuration::from_millis(100)) => {}
                        _ = shutdown.cancelled() => break,
                    }
                }
            }
        }
    }

    async fn fetch_or_populate_json<F>(
        &self,
        org_id: Uuid,
        cache_key: String,
        query_type: &'static str,
        ttl_seconds: u64,
        builder: F,
    ) -> Result<QueryExecution>
    where
        F: FnOnce() -> Result<serde_json::Value>,
    {
        match self
            .cache
            .get::<serde_json::Value>(&cache_key, self.telemetry.metrics())
            .await
        {
            Ok(Some(payload)) => {
                return Ok(QueryExecution {
                    query_type,
                    cache_hit: true,
                    payload,
                });
            }
            Ok(None) => {}
            Err(e) => {
                debug!("Cache get error for {}: {}", query_type, e);
                self.telemetry.emit_query_error(
                    query_type,
                    org_id,
                    "cache_get_error",
                    &e.to_string(),
                    None,
                );
            }
        }

        let payload = builder()?;
        if let Err(e) = self
            .cache
            .set(&cache_key, &payload, ttl_seconds, self.telemetry.metrics())
            .await
        {
            debug!("Cache set error for {}: {}", query_type, e);
            self.telemetry.emit_query_error(
                query_type,
                org_id,
                "cache_set_error",
                &e.to_string(),
                None,
            );
        }

        Ok(QueryExecution {
            query_type,
            cache_hit: false,
            payload,
        })
    }

    fn update_query_kpis(&self, query_type: &str, payload: &serde_json::Value) {
        match query_type {
            "analytics_overview_24h" | "analytics_overview_1h" => {
                if let Some(rate) = payload.get("conversion_rate").and_then(|v| v.as_f64()) {
                    self.telemetry
                        .metrics()
                        .update_business_kpi("conversion_rate", rate);
                }
            }
            "realtime_stats" => {
                if let Some(users) = payload
                    .get("current_active_users")
                    .and_then(|value| value.as_i64())
                {
                    self.telemetry
                        .metrics()
                        .update_business_kpi("realtime_active_users", users as f64);
                }
            }
            "marketing_snapshot" => {
                if let Some(campaigns) = payload
                    .get("active_campaigns")
                    .and_then(|value| value.as_i64())
                {
                    self.telemetry
                        .metrics()
                        .update_business_kpi("active_campaigns", campaigns as f64);
                }
                if let Some(experiments) = payload
                    .get("running_experiments")
                    .and_then(|value| value.as_i64())
                {
                    self.telemetry
                        .metrics()
                        .update_business_kpi("running_experiments", experiments as f64);
                }
            }
            "commerce_snapshot" => {
                if let Some(revenue) = payload
                    .get("revenue_cents")
                    .and_then(|value| value.as_i64())
                {
                    self.telemetry
                        .metrics()
                        .update_business_kpi("revenue_cents", revenue as f64);
                }
                if let Some(active) = payload
                    .get("active_subscriptions")
                    .and_then(|value| value.as_i64())
                {
                    self.telemetry
                        .metrics()
                        .update_business_kpi("active_subscriptions", active as f64);
                }
                if let Some(rate) = payload
                    .get("payment_failure_rate")
                    .and_then(|value| value.as_f64())
                {
                    self.telemetry
                        .metrics()
                        .update_business_kpi("payment_failure_rate", rate);
                }
            }
            "session_snapshot" => {
                if let Some(users) = payload.get("active_users").and_then(|value| value.as_i64()) {
                    self.telemetry
                        .metrics()
                        .update_business_kpi("active_sessions", users as f64);
                }
            }
            _ => {}
        }
    }

    /// Execute diverse query types with weighted distribution
    async fn execute_diverse_query(&self, org_id: Uuid) -> Result<()> {
        let mut rng = StdRng::from_entropy();
        let selected_query_type = self
            .controls
            .snapshot()
            .query_distribution
            .select_query_type(&mut rng);

        let start = Instant::now();
        let result = match selected_query_type {
            "analytics_overview_24h" => self.get_analytics_overview(org_id, 24).await,
            "hourly_metrics" => {
                let hour_offset = rng.gen_range(0..24);
                self.get_hourly_metrics(org_id, hour_offset).await
            }
            "top_pages" => self.get_top_pages(org_id).await,
            "event_distribution" => self.get_event_distribution(org_id).await,
            "referrer_breakdown" => self.get_referrer_breakdown(org_id).await,
            "funnel_analysis" => self.get_funnel_analysis(org_id).await,
            "device_breakdown" => self.get_device_breakdown(org_id).await,
            "geo_breakdown" => self.get_geo_breakdown(org_id).await,
            "cohort_breakdown" => self.get_cohort_breakdown(org_id).await,
            "user_activity" => self.get_random_user_activity(org_id).await,
            "page_performance" => self.get_random_page_performance(org_id).await,
            "session_snapshot" => self.get_session_snapshot(org_id).await,
            "marketing_snapshot" => self.get_marketing_snapshot(org_id).await,
            "commerce_snapshot" => self.get_commerce_snapshot(org_id).await,
            "realtime_stats" => self.get_realtime_stats(org_id).await,
            "analytics_overview_1h" => self.get_analytics_overview(org_id, 1).await,
            _ => self.get_analytics_overview(org_id, 24).await,
        };
        let latency_ns = start.elapsed().as_nanos() as u64;
        self.telemetry.metrics().record_live_latency_ns(latency_ns);

        match result {
            Ok(query) => {
                self.telemetry
                    .metrics()
                    .record_operation_success("analytics_query");
                self.telemetry.metrics().record_query_execution(
                    query.query_type,
                    latency_ns as f64 / 1_000_000_000.0,
                    query.cache_hit,
                );
                self.update_query_kpis(query.query_type, &query.payload);
                self.telemetry.emit_query_result(
                    query.query_type,
                    org_id,
                    query.cache_hit,
                    latency_ns,
                    &query.payload,
                );
            }
            Err(e) => {
                self.telemetry
                    .metrics()
                    .record_operation_error("analytics_query", "execution_error");
                self.telemetry.emit_query_error(
                    selected_query_type,
                    org_id,
                    "query_execution_error",
                    &e.to_string(),
                    Some(latency_ns),
                );
                error!("Query error for org {}: {}", org_id, e);
            }
        }

        Ok(())
    }

    async fn get_analytics_overview(&self, org_id: Uuid, hours: i32) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_overview(org_id, hours as u32);
        let query_type = if hours == 1 {
            "analytics_overview_1h"
        } else {
            "analytics_overview_24h"
        };

        self.fetch_or_populate_json(org_id, cache_key, query_type, 900, || {
            serde_json::to_value(SyntheticDataGenerator::analytics_overview(org_id, hours))
                .map_err(Into::into)
        })
        .await
    }

    async fn get_hourly_metrics(&self, org_id: Uuid, hour_offset: i32) -> Result<QueryExecution> {
        let hour = Utc::now() - Duration::hours(hour_offset as i64);
        let cache_key = self.generator.cache_key_hourly(org_id, hour);

        self.fetch_or_populate_json(org_id, cache_key, "hourly_metrics", 3600, || {
            serde_json::to_value(SyntheticDataGenerator::hourly_metrics(org_id, hour_offset))
                .map_err(Into::into)
        })
        .await
    }

    async fn get_top_pages(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_top_pages(org_id, 24);

        self.fetch_or_populate_json(org_id, cache_key, "top_pages", 1200, || {
            serde_json::to_value(SyntheticDataGenerator::top_pages()).map_err(Into::into)
        })
        .await
    }

    async fn get_event_distribution(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_event_distribution(org_id, "24h");

        self.fetch_or_populate_json(org_id, cache_key, "event_distribution", 900, || {
            serde_json::to_value(SyntheticDataGenerator::event_distribution(org_id))
                .map_err(Into::into)
        })
        .await
    }

    async fn get_referrer_breakdown(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_referrers(org_id, "24h");

        self.fetch_or_populate_json(org_id, cache_key, "referrer_breakdown", 900, || {
            serde_json::to_value(SyntheticDataGenerator::referrer_breakdown(org_id))
                .map_err(Into::into)
        })
        .await
    }

    async fn get_funnel_analysis(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_funnel(org_id, "activation");

        self.fetch_or_populate_json(org_id, cache_key, "funnel_analysis", 900, || {
            serde_json::to_value(SyntheticDataGenerator::funnel_analysis(
                org_id,
                "activation",
            ))
            .map_err(Into::into)
        })
        .await
    }

    async fn get_device_breakdown(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_device_stats(org_id, "24h");

        self.fetch_or_populate_json(org_id, cache_key, "device_breakdown", 900, || {
            serde_json::to_value(SyntheticDataGenerator::device_breakdown(org_id))
                .map_err(Into::into)
        })
        .await
    }

    async fn get_geo_breakdown(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_geo(org_id, "24h");

        self.fetch_or_populate_json(org_id, cache_key, "geo_breakdown", 900, || {
            serde_json::to_value(SyntheticDataGenerator::geo_breakdown(org_id)).map_err(Into::into)
        })
        .await
    }

    async fn get_cohort_breakdown(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_cohort(org_id, "monthly");

        self.fetch_or_populate_json(org_id, cache_key, "cohort_breakdown", 1800, || {
            serde_json::to_value(SyntheticDataGenerator::cohort_breakdown(org_id))
                .map_err(Into::into)
        })
        .await
    }

    async fn get_random_user_activity(&self, org_id: Uuid) -> Result<QueryExecution> {
        let user_ids = self.org_cache.get_user_ids(org_id).await;
        let user_id = if user_ids.is_empty() {
            Uuid::new_v4()
        } else {
            user_ids[StdRng::from_entropy().gen_range(0..user_ids.len())]
        };
        let cache_key = self.generator.cache_key_user_activity(user_id);

        self.fetch_or_populate_json(org_id, cache_key, "user_activity", 1800, || {
            serde_json::to_value(SyntheticDataGenerator::user_activity(user_id, org_id))
                .map_err(Into::into)
        })
        .await
    }

    async fn get_random_page_performance(&self, org_id: Uuid) -> Result<QueryExecution> {
        let pages = self.generator.get_popular_pages();
        let page = pages[StdRng::from_entropy().gen_range(0..pages.len())];
        let page_url = format!("https://app.example.com{}", page);
        let cache_key = self.generator.cache_key_page(org_id, &page_url);

        self.fetch_or_populate_json(org_id, cache_key, "page_performance", 1800, || {
            serde_json::to_value(SyntheticDataGenerator::page_performance(org_id, &page_url))
                .map_err(Into::into)
        })
        .await
    }

    async fn get_session_snapshot(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_session(org_id, "active");
        let user_ids = self.org_cache.get_user_ids(org_id).await;

        self.fetch_or_populate_json(org_id, cache_key, "session_snapshot", 300, || {
            serde_json::to_value(SyntheticDataGenerator::session_snapshot(
                self.generator.as_ref(),
                org_id,
                &user_ids,
            ))
            .map_err(Into::into)
        })
        .await
    }

    async fn get_marketing_snapshot(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_marketing(org_id);

        self.fetch_or_populate_json(org_id, cache_key, "marketing_snapshot", 1800, || {
            serde_json::to_value(SyntheticDataGenerator::marketing_snapshot(
                self.generator.as_ref(),
                org_id,
            ))
            .map_err(Into::into)
        })
        .await
    }

    async fn get_commerce_snapshot(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_commerce(org_id);
        let user_ids = self.org_cache.get_user_ids(org_id).await;

        self.fetch_or_populate_json(org_id, cache_key, "commerce_snapshot", 900, || {
            serde_json::to_value(SyntheticDataGenerator::commerce_snapshot(
                self.generator.as_ref(),
                org_id,
                &user_ids,
            ))
            .map_err(Into::into)
        })
        .await
    }

    async fn get_realtime_stats(&self, org_id: Uuid) -> Result<QueryExecution> {
        let cache_key = self.generator.cache_key_realtime(org_id);

        self.fetch_or_populate_json(org_id, cache_key, "realtime_stats", 60, || {
            Ok(SyntheticDataGenerator::realtime_stats(org_id))
        })
        .await
    }
}

/// CacheWarmupWorker - Pre-populates cache with synthetic data (no DB)
pub struct CacheWarmupWorker {
    cache: Arc<RedisCache>,
    telemetry: Arc<TelemetryRuntime>,
    generator: Arc<DataGenerator>,
    org_cache: Arc<OrgIdCache>,
}

impl CacheWarmupWorker {
    pub fn new(
        cache: Arc<RedisCache>,
        telemetry: Arc<TelemetryRuntime>,
        generator: Arc<DataGenerator>,
        org_cache: Arc<OrgIdCache>,
    ) -> Self {
        Self {
            cache,
            telemetry,
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
                let user_ids = self.org_cache.get_user_ids(org_id).await;

                for hours in [1, 6, 24, 168] {
                    let data = SyntheticDataGenerator::analytics_overview(org_id, hours);
                    push_json_entry(
                        &mut batch_entries,
                        self.generator.cache_key_overview(org_id, hours as u32),
                        &data,
                        900,
                    );
                }

                for hour_offset in 0..24 {
                    let data = SyntheticDataGenerator::hourly_metrics(org_id, hour_offset);
                    let hour = Utc::now() - Duration::hours(hour_offset as i64);
                    push_json_entry(
                        &mut batch_entries,
                        self.generator.cache_key_hourly(org_id, hour),
                        &data,
                        3600,
                    );
                }

                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_top_pages(org_id, 24),
                    &SyntheticDataGenerator::top_pages(),
                    1200,
                );
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_event_distribution(org_id, "24h"),
                    &SyntheticDataGenerator::event_distribution(org_id),
                    900,
                );
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_referrers(org_id, "24h"),
                    &SyntheticDataGenerator::referrer_breakdown(org_id),
                    900,
                );
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_cohort(org_id, "monthly"),
                    &SyntheticDataGenerator::cohort_breakdown(org_id),
                    1800,
                );
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_device_stats(org_id, "24h"),
                    &SyntheticDataGenerator::device_breakdown(org_id),
                    900,
                );
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_geo(org_id, "24h"),
                    &SyntheticDataGenerator::geo_breakdown(org_id),
                    900,
                );
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_funnel(org_id, "activation"),
                    &SyntheticDataGenerator::funnel_analysis(org_id, "activation"),
                    900,
                );
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_session(org_id, "active"),
                    &SyntheticDataGenerator::session_snapshot(
                        self.generator.as_ref(),
                        org_id,
                        &user_ids,
                    ),
                    300,
                );
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_marketing(org_id),
                    &SyntheticDataGenerator::marketing_snapshot(self.generator.as_ref(), org_id),
                    1800,
                );
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_commerce(org_id),
                    &SyntheticDataGenerator::commerce_snapshot(
                        self.generator.as_ref(),
                        org_id,
                        &user_ids,
                    ),
                    900,
                );

                for page in self.generator.get_popular_pages() {
                    let page_url = format!("https://app.example.com{}", page);
                    let data = SyntheticDataGenerator::page_performance(org_id, &page_url);
                    push_json_entry(
                        &mut batch_entries,
                        self.generator.cache_key_page(org_id, &page_url),
                        &data,
                        1800,
                    );
                }

                for user_id in user_ids.iter().take(20) {
                    let data = SyntheticDataGenerator::user_activity(*user_id, org_id);
                    push_json_entry(
                        &mut batch_entries,
                        self.generator.cache_key_user_activity(*user_id),
                        &data,
                        1800,
                    );
                }

                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_realtime(org_id),
                    &SyntheticDataGenerator::realtime_stats(org_id),
                    60,
                );

                for minutes in [5, 15, 30, 60] {
                    let data = json!({
                        "count": StdRng::from_entropy().gen_range(100..10000),
                        "window_minutes": minutes
                    });
                    push_json_entry(
                        &mut batch_entries,
                        self.generator
                            .cache_key_rolling_window(org_id, "events", minutes),
                        &data,
                        (minutes * 60) as u64,
                    );
                }
            }

            let chunk_size = batch_entries.len();
            if !batch_entries.is_empty() {
                if let Err(e) = self
                    .cache
                    .set_batch_json(batch_entries, self.telemetry.metrics())
                    .await
                {
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
            total_keys,
            org_count,
            duration,
            total_keys as f64 / duration
        );

        self.telemetry.emit_cache_warmup(&CacheWarmupSummary {
            phase: "bulk_populate".to_string(),
            organizations: org_count,
            keys_written: total_keys,
            duration_seconds: duration,
        });

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
            let user_ids = self.org_cache.get_user_ids(org_id).await;

            for hours in [1, 6, 24] {
                let data = SyntheticDataGenerator::analytics_overview(org_id, hours);
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_overview(org_id, hours as u32),
                    &data,
                    900,
                );
                refreshed_count += 1;
            }

            for hour_offset in 0..6 {
                let data = SyntheticDataGenerator::hourly_metrics(org_id, hour_offset);
                let hour = Utc::now() - Duration::hours(hour_offset as i64);
                push_json_entry(
                    &mut batch_entries,
                    self.generator.cache_key_hourly(org_id, hour),
                    &data,
                    3600,
                );
                refreshed_count += 1;
            }

            push_json_entry(
                &mut batch_entries,
                self.generator.cache_key_referrers(org_id, "24h"),
                &SyntheticDataGenerator::referrer_breakdown(org_id),
                900,
            );
            push_json_entry(
                &mut batch_entries,
                self.generator.cache_key_device_stats(org_id, "24h"),
                &SyntheticDataGenerator::device_breakdown(org_id),
                900,
            );
            push_json_entry(
                &mut batch_entries,
                self.generator.cache_key_geo(org_id, "24h"),
                &SyntheticDataGenerator::geo_breakdown(org_id),
                900,
            );
            push_json_entry(
                &mut batch_entries,
                self.generator.cache_key_session(org_id, "active"),
                &SyntheticDataGenerator::session_snapshot(
                    self.generator.as_ref(),
                    org_id,
                    &user_ids,
                ),
                300,
            );
            push_json_entry(
                &mut batch_entries,
                self.generator.cache_key_marketing(org_id),
                &SyntheticDataGenerator::marketing_snapshot(self.generator.as_ref(), org_id),
                1800,
            );
            push_json_entry(
                &mut batch_entries,
                self.generator.cache_key_commerce(org_id),
                &SyntheticDataGenerator::commerce_snapshot(
                    self.generator.as_ref(),
                    org_id,
                    &user_ids,
                ),
                900,
            );

            if batch_entries.len() >= 100 {
                if let Err(e) = self
                    .cache
                    .set_batch_json(batch_entries.clone(), self.telemetry.metrics())
                    .await
                {
                    error!("Batch cache write failed: {}", e);
                }
                batch_entries.clear();
            }
        }

        if !batch_entries.is_empty() {
            if let Err(e) = self
                .cache
                .set_batch_json(batch_entries, self.telemetry.metrics())
                .await
            {
                error!("Final batch cache write failed: {}", e);
            }
        }

        let duration = start.elapsed().as_secs_f64();
        debug!(
            "Cache warmup completed: {} entries in {:.2}s",
            refreshed_count, duration
        );

        self.telemetry.emit_cache_warmup(&CacheWarmupSummary {
            phase: "refresh".to_string(),
            organizations: self.org_cache.get_org_ids().await.len(),
            keys_written: refreshed_count,
            duration_seconds: duration,
        });

        Ok(())
    }
}

/// EventSimulatorWorker - Writes up to 1M keys with exponential backoff
///
/// Starts at 100% writes, exponentially decays to ~1% as keys accumulate.
/// Uses pipelined SET (writes) and GET (reads) for high throughput.
pub struct EventSimulatorWorker {
    cache: Arc<RedisCache>,
    telemetry: Arc<TelemetryRuntime>,
    generator: Arc<DataGenerator>,
    controls: Arc<RuntimeControls>,
    org_cache: Arc<OrgIdCache>,
    keys_written: Arc<AtomicU64>,
    write_decay_tau: f64,
    batch_count: AtomicU64,
}

const MIN_WRITE_RATIO: f64 = 0.01;

impl EventSimulatorWorker {
    pub fn new(
        cache: Arc<RedisCache>,
        telemetry: Arc<TelemetryRuntime>,
        generator: Arc<DataGenerator>,
        controls: Arc<RuntimeControls>,
        org_cache: Arc<OrgIdCache>,
        keys_written: Arc<AtomicU64>,
        target_keys: u64,
    ) -> Self {
        let write_decay_tau = target_keys as f64 / (1.0 / MIN_WRITE_RATIO).ln();
        Self {
            cache,
            telemetry,
            generator,
            controls,
            org_cache,
            keys_written,
            write_decay_tau,
            batch_count: AtomicU64::new(0),
        }
    }

    /// Run a batch of mixed write/read operations with exponential write decay.
    pub async fn run_batch(&self, ops_per_second: u64) -> Result<()> {
        let start = Instant::now();
        let org_ids = self.org_cache.get_org_ids().await;

        if org_ids.is_empty() {
            warn!("No organizations available for event simulation");
            return Ok(());
        }

        let mut rng = StdRng::from_entropy();
        let current_keys = self.keys_written.load(Ordering::Relaxed);

        let write_ratio = f64::max(
            MIN_WRITE_RATIO,
            (-(current_keys as f64) / self.write_decay_tau).exp(),
        );
        let num_writes = (ops_per_second as f64 * write_ratio) as u64;
        let num_reads = ops_per_second.saturating_sub(num_writes);

        if num_writes > 0 {
            let mut write_entries: Vec<(String, String, u64)> =
                Vec::with_capacity(num_writes as usize);
            let base_counter = self.keys_written.fetch_add(num_writes, Ordering::Relaxed);

            for i in 0..num_writes {
                let org_id = org_ids[rng.gen_range(0..org_ids.len())];
                let key = format!("data:{}:{}", org_id, base_counter + i);
                let value = format!(
                    "{{\"v\":{},\"ts\":{}}}",
                    rng.gen_range(1..1_000_000i64),
                    Utc::now().timestamp()
                );
                write_entries.push((key, value, 3600));
            }

            if let Err(e) = self
                .cache
                .set_batch_json(write_entries, self.telemetry.metrics())
                .await
            {
                warn!("Batch write failed: {}", e);
            }
        }

        if num_reads > 0 && current_keys > 0 {
            let mut read_keys: Vec<String> = Vec::with_capacity(num_reads as usize);

            for _ in 0..num_reads {
                let org_id = org_ids[rng.gen_range(0..org_ids.len())];
                let key_idx = rng.gen_range(0..current_keys);
                read_keys.push(format!("data:{}:{}", org_id, key_idx));
            }

            if let Err(e) = self
                .cache
                .get_batch(&read_keys, self.telemetry.metrics())
                .await
            {
                warn!("Batch read failed: {}", e);
            }
        }

        let duration = start.elapsed().as_secs_f64();
        self.telemetry
            .metrics()
            .record_event_generation_duration(duration);
        self.telemetry
            .metrics()
            .update_business_kpi("write_ratio_percent", write_ratio * 100.0);

        let mut breakdown = [0u64; 5];
        let event_distribution = self.controls.snapshot().event_distribution;

        for _ in 0..num_writes {
            match event_distribution.select_event_type(&mut rng) {
                "page_view" => {
                    self.telemetry.metrics().record_event_generated("page_view");
                    breakdown[0] += 1;
                }
                "click" => {
                    self.telemetry.metrics().record_event_generated("click");
                    breakdown[1] += 1;
                }
                "conversion" => {
                    self.telemetry
                        .metrics()
                        .record_event_generated("conversion");
                    breakdown[2] += 1;
                }
                "sign_up" => {
                    self.telemetry.metrics().record_event_generated("sign_up");
                    breakdown[3] += 1;
                }
                "purchase" => {
                    self.telemetry.metrics().record_event_generated("purchase");
                    breakdown[4] += 1;
                }
                _ => {}
            }
        }

        let total_keys = self.keys_written.load(Ordering::Relaxed);
        self.telemetry
            .metrics()
            .update_business_kpi("redis_total_keys", total_keys as f64);

        let sample_org = org_ids[rng.gen_range(0..org_ids.len())];
        let sample_user_ids = self.org_cache.get_user_ids(sample_org).await;
        let samples = SyntheticDataGenerator::event_batch_samples(
            self.generator.as_ref(),
            sample_org,
            &sample_user_ids,
            self.telemetry.event_sample_size(),
        );

        self.telemetry.emit_event_batch(&EventBatchSummary {
            operations_per_second: ops_per_second,
            writes: num_writes,
            reads: num_reads,
            write_ratio,
            total_keys,
            duration_ms: duration * 1000.0,
            event_type_breakdown: json!({
                "page_view": breakdown[0],
                "click": breakdown[1],
                "conversion": breakdown[2],
                "sign_up": breakdown[3],
                "purchase": breakdown[4],
            }),
            samples,
        });

        let batch = self.batch_count.fetch_add(1, Ordering::Relaxed);
        if batch.is_multiple_of(10) {
            info!(
                "Redis ops: {} writes + {} reads in {:.1}ms | write_ratio={:.0}% | total_keys={}",
                num_writes,
                num_reads,
                duration * 1000.0,
                write_ratio * 100.0,
                total_keys
            );
        }

        Ok(())
    }
}
