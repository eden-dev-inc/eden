// Runtime Controls
//
// Provides live-tunable throughput and distribution settings that can be
// changed over HTTP without rebuilding or restarting the app.

use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use tokio::sync::Semaphore;
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;

use crate::Config;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDistribution {
    pub analytics_overview_24h: u32,
    pub analytics_overview_1h: u32,
    pub hourly_metrics: u32,
    pub top_pages: u32,
    pub event_distribution: u32,
    pub referrer_breakdown: u32,
    pub funnel_analysis: u32,
    pub device_breakdown: u32,
    pub geo_breakdown: u32,
    pub cohort_breakdown: u32,
    pub user_activity: u32,
    pub page_performance: u32,
    pub session_snapshot: u32,
    pub marketing_snapshot: u32,
    pub commerce_snapshot: u32,
    pub realtime_stats: u32,
}

impl Default for QueryDistribution {
    fn default() -> Self {
        Self {
            analytics_overview_24h: 20,
            analytics_overview_1h: 1,
            hourly_metrics: 10,
            top_pages: 6,
            event_distribution: 6,
            referrer_breakdown: 5,
            funnel_analysis: 5,
            device_breakdown: 5,
            geo_breakdown: 5,
            cohort_breakdown: 5,
            user_activity: 5,
            page_performance: 5,
            session_snapshot: 6,
            marketing_snapshot: 7,
            commerce_snapshot: 6,
            realtime_stats: 3,
        }
    }
}

impl QueryDistribution {
    pub fn total_weight(&self) -> u32 {
        self.analytics_overview_24h
            + self.analytics_overview_1h
            + self.hourly_metrics
            + self.top_pages
            + self.event_distribution
            + self.referrer_breakdown
            + self.funnel_analysis
            + self.device_breakdown
            + self.geo_breakdown
            + self.cohort_breakdown
            + self.user_activity
            + self.page_performance
            + self.session_snapshot
            + self.marketing_snapshot
            + self.commerce_snapshot
            + self.realtime_stats
    }

    pub fn select_query_type<R: Rng + ?Sized>(&self, rng: &mut R) -> &'static str {
        let total = self.total_weight().max(1);
        let mut roll = rng.gen_range(0..total);
        for (query_type, weight) in self.weighted_entries() {
            if roll < weight {
                return query_type;
            }
            roll -= weight;
        }

        "analytics_overview_24h"
    }

    fn weighted_entries(&self) -> [(&'static str, u32); 16] {
        [
            ("analytics_overview_24h", self.analytics_overview_24h),
            ("analytics_overview_1h", self.analytics_overview_1h),
            ("hourly_metrics", self.hourly_metrics),
            ("top_pages", self.top_pages),
            ("event_distribution", self.event_distribution),
            ("referrer_breakdown", self.referrer_breakdown),
            ("funnel_analysis", self.funnel_analysis),
            ("device_breakdown", self.device_breakdown),
            ("geo_breakdown", self.geo_breakdown),
            ("cohort_breakdown", self.cohort_breakdown),
            ("user_activity", self.user_activity),
            ("page_performance", self.page_performance),
            ("session_snapshot", self.session_snapshot),
            ("marketing_snapshot", self.marketing_snapshot),
            ("commerce_snapshot", self.commerce_snapshot),
            ("realtime_stats", self.realtime_stats),
        ]
    }

    fn apply_patch(&mut self, patch: QueryDistributionPatch) {
        if let Some(value) = patch.analytics_overview_24h {
            self.analytics_overview_24h = value;
        }
        if let Some(value) = patch.analytics_overview_1h {
            self.analytics_overview_1h = value;
        }
        if let Some(value) = patch.hourly_metrics {
            self.hourly_metrics = value;
        }
        if let Some(value) = patch.top_pages {
            self.top_pages = value;
        }
        if let Some(value) = patch.event_distribution {
            self.event_distribution = value;
        }
        if let Some(value) = patch.referrer_breakdown {
            self.referrer_breakdown = value;
        }
        if let Some(value) = patch.funnel_analysis {
            self.funnel_analysis = value;
        }
        if let Some(value) = patch.device_breakdown {
            self.device_breakdown = value;
        }
        if let Some(value) = patch.geo_breakdown {
            self.geo_breakdown = value;
        }
        if let Some(value) = patch.cohort_breakdown {
            self.cohort_breakdown = value;
        }
        if let Some(value) = patch.user_activity {
            self.user_activity = value;
        }
        if let Some(value) = patch.page_performance {
            self.page_performance = value;
        }
        if let Some(value) = patch.session_snapshot {
            self.session_snapshot = value;
        }
        if let Some(value) = patch.marketing_snapshot {
            self.marketing_snapshot = value;
        }
        if let Some(value) = patch.commerce_snapshot {
            self.commerce_snapshot = value;
        }
        if let Some(value) = patch.realtime_stats {
            self.realtime_stats = value;
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct QueryDistributionPatch {
    pub analytics_overview_24h: Option<u32>,
    pub analytics_overview_1h: Option<u32>,
    pub hourly_metrics: Option<u32>,
    pub top_pages: Option<u32>,
    pub event_distribution: Option<u32>,
    pub referrer_breakdown: Option<u32>,
    pub funnel_analysis: Option<u32>,
    pub device_breakdown: Option<u32>,
    pub geo_breakdown: Option<u32>,
    pub cohort_breakdown: Option<u32>,
    pub user_activity: Option<u32>,
    pub page_performance: Option<u32>,
    pub session_snapshot: Option<u32>,
    pub marketing_snapshot: Option<u32>,
    pub commerce_snapshot: Option<u32>,
    pub realtime_stats: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDistribution {
    pub page_view: u32,
    pub click: u32,
    pub conversion: u32,
    pub sign_up: u32,
    pub purchase: u32,
}

impl Default for EventDistribution {
    fn default() -> Self {
        Self {
            page_view: 60,
            click: 28,
            conversion: 8,
            sign_up: 3,
            purchase: 1,
        }
    }
}

impl EventDistribution {
    pub fn total_weight(&self) -> u32 {
        self.page_view + self.click + self.conversion + self.sign_up + self.purchase
    }

    pub fn select_event_type<R: Rng + ?Sized>(&self, rng: &mut R) -> &'static str {
        let total = self.total_weight().max(1);
        let mut roll = rng.gen_range(0..total);
        for (event_type, weight) in self.weighted_entries() {
            if roll < weight {
                return event_type;
            }
            roll -= weight;
        }

        "page_view"
    }

    fn weighted_entries(&self) -> [(&'static str, u32); 5] {
        [
            ("page_view", self.page_view),
            ("click", self.click),
            ("conversion", self.conversion),
            ("sign_up", self.sign_up),
            ("purchase", self.purchase),
        ]
    }

    fn apply_patch(&mut self, patch: EventDistributionPatch) {
        if let Some(value) = patch.page_view {
            self.page_view = value;
        }
        if let Some(value) = patch.click {
            self.click = value;
        }
        if let Some(value) = patch.conversion {
            self.conversion = value;
        }
        if let Some(value) = patch.sign_up {
            self.sign_up = value;
        }
        if let Some(value) = patch.purchase {
            self.purchase = value;
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct EventDistributionPatch {
    pub page_view: Option<u32>,
    pub click: Option<u32>,
    pub conversion: Option<u32>,
    pub sign_up: Option<u32>,
    pub purchase: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeControlSettings {
    pub queries_per_second: u64,
    pub events_per_second: u64,
    pub query_distribution: QueryDistribution,
    pub event_distribution: EventDistribution,
}

impl RuntimeControlSettings {
    pub fn from_config(config: &Config) -> Self {
        Self {
            queries_per_second: config.queries_per_second,
            events_per_second: config.events_per_second,
            query_distribution: QueryDistribution::default(),
            event_distribution: EventDistribution::default(),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.query_distribution.total_weight() == 0 {
            return Err("query_distribution must have at least one non-zero weight".to_string());
        }

        if self.event_distribution.total_weight() == 0 {
            return Err("event_distribution must have at least one non-zero weight".to_string());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RuntimeControlPatch {
    pub queries_per_second: Option<u64>,
    pub events_per_second: Option<u64>,
    pub query_distribution: Option<QueryDistributionPatch>,
    pub event_distribution: Option<EventDistributionPatch>,
}

pub struct RuntimeControls {
    settings: RwLock<RuntimeControlSettings>,
    query_limiter: Arc<AdaptiveRateLimiter>,
}

impl RuntimeControls {
    pub fn from_config(config: &Config) -> Arc<Self> {
        Arc::new(Self {
            settings: RwLock::new(RuntimeControlSettings::from_config(config)),
            query_limiter: Arc::new(AdaptiveRateLimiter::new()),
        })
    }

    pub fn snapshot(&self) -> RuntimeControlSettings {
        self.settings
            .read()
            .expect("runtime controls lock poisoned")
            .clone()
    }

    pub fn apply_patch(
        &self,
        patch: RuntimeControlPatch,
    ) -> Result<RuntimeControlSettings, String> {
        let mut next = self.snapshot();

        if let Some(value) = patch.queries_per_second {
            next.queries_per_second = value;
        }
        if let Some(value) = patch.events_per_second {
            next.events_per_second = value;
        }
        if let Some(distribution_patch) = patch.query_distribution {
            next.query_distribution.apply_patch(distribution_patch);
        }
        if let Some(distribution_patch) = patch.event_distribution {
            next.event_distribution.apply_patch(distribution_patch);
        }

        next.validate()?;
        *self
            .settings
            .write()
            .expect("runtime controls lock poisoned") = next.clone();
        Ok(next)
    }

    pub fn start_background_tasks(self: &Arc<Self>, shutdown: CancellationToken) {
        let controls = self.clone();
        let limiter = self.query_limiter.clone();
        let query_shutdown = shutdown.clone();
        tokio::spawn(async move {
            limiter.run_refill_loop(controls, query_shutdown).await;
        });
    }

    pub fn query_limiter(&self) -> Arc<AdaptiveRateLimiter> {
        self.query_limiter.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EventDistributionPatch, QueryDistributionPatch, RuntimeControlPatch, RuntimeControls,
    };
    use crate::Config;

    fn base_config() -> Config {
        Config {
            redis_enabled: true,
            redis_url: "redis://localhost:6379".to_string(),
            postgres_enabled: false,
            allow_no_backend: false,
            bind_address: "127.0.0.1:3000".to_string(),
            events_per_second: 100,
            redis_target_keys: 1000,
            queries_per_second: 250,
            internal_workload_enabled: false,
            organizations: 10,
            users_per_org: 50,
            cache_hit_target: 95,
            max_workers: 16,
            redis_pool_size: 8,
            cache_ttl: 300,
            warmup_interval: 300,
            time_buckets: 24,
            telemetry_provider: crate::telemetry::TelemetryProvider::Datadog,
            telemetry_enabled: false,
            telemetry_service: "analytics-server".to_string(),
            telemetry_env: "test".to_string(),
            telemetry_version: "0.0.0-test".to_string(),
            telemetry_site: "datadoghq.com".to_string(),
            telemetry_datadog_api_key: None,
            telemetry_dogstatsd_endpoint: None,
            telemetry_export_interval_seconds: 10,
            telemetry_opentelemetry_endpoint: None,
            telemetry_otlp_export_interval_seconds: 10,
            telemetry_otlp_timeout_seconds: 5,
            telemetry_query_log_every: 10,
            telemetry_event_sample_size: 2,
            telemetry_capture_query_payloads: true,
            telemetry_capture_event_payloads: true,
            telemetry_capture_system_snapshots: false,
            postgres_host: "localhost".to_string(),
            postgres_port: 5432,
            postgres_user: "postgres".to_string(),
            postgres_password: "postgres".to_string(),
            postgres_database: "analytics".to_string(),
            database_url: None,
            db_pool_size: 5,
            pg_query_workers: 2,
            pg_events_per_second: 10,
        }
    }

    #[test]
    fn apply_patch_updates_runtime_settings() {
        let controls = RuntimeControls::from_config(&base_config());

        let updated = controls
            .apply_patch(RuntimeControlPatch {
                queries_per_second: Some(777),
                events_per_second: Some(55),
                query_distribution: Some(QueryDistributionPatch {
                    hourly_metrics: Some(99),
                    ..Default::default()
                }),
                event_distribution: Some(EventDistributionPatch {
                    purchase: Some(7),
                    ..Default::default()
                }),
            })
            .expect("patch should succeed");

        assert_eq!(updated.queries_per_second, 777);
        assert_eq!(updated.events_per_second, 55);
        assert_eq!(updated.query_distribution.hourly_metrics, 99);
        assert_eq!(updated.event_distribution.purchase, 7);
    }

    #[test]
    fn apply_patch_rejects_zero_weight_query_distribution() {
        let controls = RuntimeControls::from_config(&base_config());

        let error = controls
            .apply_patch(RuntimeControlPatch {
                query_distribution: Some(QueryDistributionPatch {
                    analytics_overview_24h: Some(0),
                    analytics_overview_1h: Some(0),
                    hourly_metrics: Some(0),
                    top_pages: Some(0),
                    event_distribution: Some(0),
                    referrer_breakdown: Some(0),
                    funnel_analysis: Some(0),
                    device_breakdown: Some(0),
                    geo_breakdown: Some(0),
                    cohort_breakdown: Some(0),
                    user_activity: Some(0),
                    page_performance: Some(0),
                    session_snapshot: Some(0),
                    marketing_snapshot: Some(0),
                    commerce_snapshot: Some(0),
                    realtime_stats: Some(0),
                }),
                ..Default::default()
            })
            .expect_err("patch should fail");

        assert!(error.contains("query_distribution"));
    }
}

pub struct AdaptiveRateLimiter {
    permits: Arc<Semaphore>,
}

impl AdaptiveRateLimiter {
    fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(0)),
        }
    }

    pub async fn acquire_until(&self, shutdown: &CancellationToken) -> bool {
        tokio::select! {
            result = self.permits.acquire() => {
                let permit = result.expect("query rate limiter semaphore closed");
                permit.forget();
                true
            }
            _ = shutdown.cancelled() => false,
        }
    }

    async fn run_refill_loop(&self, controls: Arc<RuntimeControls>, shutdown: CancellationToken) {
        let mut ticker = time::interval(Duration::from_millis(100));
        let mut carry = 0.0f64;

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.cancelled() => break,
            }
            let snapshot = controls.snapshot();
            let rate = snapshot.queries_per_second as f64;
            let permits_per_tick = rate / 10.0;
            let desired = permits_per_tick + carry;
            let permits_to_add = desired.floor() as usize;
            carry = desired - permits_to_add as f64;

            let desired_capacity = if snapshot.queries_per_second == 0 {
                0
            } else {
                ((permits_per_tick.ceil() as usize).max(1)) * 2
            };
            self.trim_available_permits(desired_capacity);

            if permits_to_add == 0 {
                continue;
            }

            let available = self.permits.available_permits();
            if available < desired_capacity {
                self.permits
                    .add_permits((desired_capacity - available).min(permits_to_add));
            }
        }
    }

    fn trim_available_permits(&self, desired_capacity: usize) {
        let mut available = self.permits.available_permits();
        while available > desired_capacity {
            let to_remove = (available - desired_capacity).min(u32::MAX as usize);
            match self
                .permits
                .clone()
                .try_acquire_many_owned(to_remove as u32)
            {
                Ok(permit) => {
                    permit.forget();
                    available = self.permits.available_permits();
                }
                Err(_) => break,
            }
        }
    }
}

#[derive(Default)]
pub struct RateAccumulator {
    carry: f64,
}

impl RateAccumulator {
    pub fn take_for_tick(&mut self, per_second: u64, tick: Duration) -> u64 {
        let desired = per_second as f64 * tick.as_secs_f64() + self.carry;
        let whole = desired.floor() as u64;
        self.carry = desired - whole as f64;
        whole
    }
}
