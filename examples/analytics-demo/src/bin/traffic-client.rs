use analytics_server::{
    activity,
    models::Organization,
    telemetry::{
        init_tracing, install_legacy_telemetry_env_aliases, parse_telemetry_provider,
        wait_for_shutdown_signal, ActivityEmission, TelemetryOptions, TelemetryProvider,
        TelemetryRuntime, TelemetrySpan, TelemetrySpanKind,
    },
};
use anyhow::{anyhow, Result as AnyResult};
use axum::{
    body::Body,
    extract::{MatchedPath, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::get,
    Json, Router,
};
use chrono::Utc;
use clap::Parser;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock as AsyncRwLock, Semaphore};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

fn parse_bool(value: &str) -> Result<bool, String> {
    value
        .parse::<bool>()
        .map_err(|error| format!("expected true or false, got '{value}': {error}"))
}

fn default_client_instance_id() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string())
}

#[derive(Parser, Debug, Clone)]
#[clap(name = "traffic-client")]
#[clap(about = "External traffic client for analytics-server")]
struct TrafficClientConfig {
    #[clap(long, env = "CLIENT_NAME", default_value = "traffic-client")]
    client_name: String,

    #[clap(
        long,
        env = "CLIENT_INSTANCE_ID",
        default_value_t = default_client_instance_id()
    )]
    client_instance_id: String,

    #[clap(long, env = "CLIENT_PROFILE", default_value = "balanced")]
    client_profile: String,

    #[clap(long, env = "CLIENT_BIND_ADDRESS", default_value = "0.0.0.0:3100")]
    bind_address: String,

    #[clap(long, env = "TARGET_BASE_URL", default_value = "http://localhost:3000")]
    target_base_url: String,

    #[clap(long, env = "QUERIES_PER_SECOND", default_value = "150")]
    queries_per_second: u64,

    #[clap(long, env = "EVENTS_PER_SECOND", default_value = "25")]
    events_per_second: u64,

    #[clap(long, env = "QUERY_WORKERS", default_value = "8")]
    query_workers: usize,

    #[clap(long, env = "EVENT_WORKERS", default_value = "4")]
    event_workers: usize,

    #[clap(long, env = "ORGANIZATION_FETCH_LIMIT", default_value = "100")]
    organization_fetch_limit: u32,

    #[clap(
        long,
        env = "ORGANIZATION_REFRESH_INTERVAL_SECONDS",
        default_value = "30"
    )]
    organization_refresh_interval_seconds: u64,

    #[clap(long, env = "REQUEST_TIMEOUT_MS", default_value = "5000")]
    request_timeout_ms: u64,

    #[clap(
        long = "telemetry-provider",
        alias = "datadog-provider",
        env = "TELEMETRY_PROVIDER",
        default_value = "datadog",
        parse(try_from_str = parse_telemetry_provider)
    )]
    telemetry_provider: TelemetryProvider,

    #[clap(
        long = "telemetry-enabled",
        alias = "datadog-enabled",
        env = "TELEMETRY_ENABLED",
        default_value = "false",
        parse(try_from_str = parse_bool)
    )]
    telemetry_enabled: bool,

    #[clap(
        long = "telemetry-service",
        alias = "datadog-service",
        env = "TELEMETRY_SERVICE",
        default_value = "traffic-client"
    )]
    telemetry_service: String,

    #[clap(
        long = "telemetry-env",
        alias = "datadog-env",
        env = "TELEMETRY_ENV",
        default_value = "demo"
    )]
    telemetry_env: String,

    #[clap(
        long = "telemetry-version",
        alias = "datadog-version",
        env = "TELEMETRY_VERSION",
        default_value = "0.1.0"
    )]
    telemetry_version: String,

    #[clap(
        long = "telemetry-site",
        alias = "datadog-site",
        env = "TELEMETRY_SITE",
        default_value = "datadoghq.com"
    )]
    telemetry_site: String,

    #[clap(
        long = "telemetry-datadog-api-key",
        alias = "datadog-api-key",
        env = "TELEMETRY_DATADOG_API_KEY"
    )]
    telemetry_datadog_api_key: Option<String>,

    #[clap(
        long = "telemetry-dogstatsd-endpoint",
        env = "TELEMETRY_DOGSTATSD_ENDPOINT"
    )]
    telemetry_dogstatsd_endpoint: Option<String>,

    #[clap(
        long = "telemetry-export-interval-seconds",
        env = "TELEMETRY_EXPORT_INTERVAL_SECONDS",
        default_value = "10"
    )]
    telemetry_export_interval_seconds: u64,

    #[clap(
        long = "telemetry-opentelemetry-endpoint",
        alias = "telemetry-otlp-traces-endpoint",
        env = "TELEMETRY_OPENTELEMETRY_ENDPOINT"
    )]
    telemetry_opentelemetry_endpoint: Option<String>,

    #[clap(
        long = "telemetry-otlp-export-interval-seconds",
        env = "TELEMETRY_OTLP_EXPORT_INTERVAL_SECONDS",
        default_value = "10"
    )]
    telemetry_otlp_export_interval_seconds: u64,

    #[clap(
        long = "telemetry-otlp-timeout-seconds",
        env = "TELEMETRY_OTLP_TIMEOUT_SECONDS",
        default_value = "5"
    )]
    telemetry_otlp_timeout_seconds: u64,

    #[clap(
        long = "telemetry-query-log-every",
        alias = "datadog-query-log-every",
        env = "TELEMETRY_QUERY_LOG_EVERY",
        default_value = "1"
    )]
    telemetry_query_log_every: u64,

    #[clap(
        long = "telemetry-event-sample-size",
        alias = "datadog-event-sample-size",
        env = "TELEMETRY_EVENT_SAMPLE_SIZE",
        default_value = "1"
    )]
    telemetry_event_sample_size: usize,

    #[clap(
        long = "telemetry-capture-query-payloads",
        alias = "datadog-capture-query-payloads",
        env = "TELEMETRY_CAPTURE_QUERY_PAYLOADS",
        default_value = "true",
        parse(try_from_str = parse_bool)
    )]
    telemetry_capture_query_payloads: bool,

    #[clap(
        long = "telemetry-capture-event-payloads",
        alias = "datadog-capture-event-payloads",
        env = "TELEMETRY_CAPTURE_EVENT_PAYLOADS",
        default_value = "true",
        parse(try_from_str = parse_bool)
    )]
    telemetry_capture_event_payloads: bool,

    #[clap(
        long = "telemetry-capture-system-snapshots",
        alias = "datadog-capture-system-snapshots",
        env = "TELEMETRY_CAPTURE_SYSTEM_SNAPSHOTS",
        default_value = "false",
        parse(try_from_str = parse_bool)
    )]
    telemetry_capture_system_snapshots: bool,
}

impl TrafficClientConfig {
    fn validate(&self) -> AnyResult<()> {
        if self.query_workers == 0 {
            return Err(anyhow!("QUERY_WORKERS must be greater than zero"));
        }
        if self.event_workers == 0 {
            return Err(anyhow!("EVENT_WORKERS must be greater than zero"));
        }
        Ok(())
    }

    fn telemetry_options(&self) -> TelemetryOptions {
        TelemetryOptions {
            provider: self.telemetry_provider,
            enabled: self.telemetry_enabled,
            service: self.telemetry_service.clone(),
            environment: self.telemetry_env.clone(),
            version: self.telemetry_version.clone(),
            site: self.telemetry_site.clone(),
            datadog_api_key: self.telemetry_datadog_api_key.clone(),
            dogstatsd_endpoint: self.telemetry_dogstatsd_endpoint.clone(),
            dogstatsd_export_interval_seconds: self.telemetry_export_interval_seconds,
            opentelemetry_endpoint: self.telemetry_opentelemetry_endpoint.clone(),
            otlp_export_interval_seconds: self.telemetry_otlp_export_interval_seconds,
            otlp_export_timeout_seconds: self.telemetry_otlp_timeout_seconds,
            query_log_every: self.telemetry_query_log_every,
            event_sample_size: self.telemetry_event_sample_size,
            capture_query_payloads: self.telemetry_capture_query_payloads,
            capture_event_payloads: self.telemetry_capture_event_payloads,
            capture_system_snapshots: self.telemetry_capture_system_snapshots,
        }
    }

    fn startup_payload(&self) -> Value {
        json!({
            "client_name": self.client_name,
            "client_instance_id": self.client_instance_id,
            "client_profile": self.client_profile,
            "bind_address": self.bind_address,
            "target_base_url": self.target_base_url,
            "queries_per_second": self.queries_per_second,
            "events_per_second": self.events_per_second,
            "query_workers": self.query_workers,
            "event_workers": self.event_workers,
            "organization_fetch_limit": self.organization_fetch_limit,
            "organization_refresh_interval_seconds": self.organization_refresh_interval_seconds,
            "request_timeout_ms": self.request_timeout_ms,
        })
    }

    fn profile(&self) -> ClientProfile {
        ClientProfile::from_str(&self.client_profile)
    }
}

fn client_identity_tags(config: &TrafficClientConfig) -> Vec<String> {
    vec![
        "role:client".to_string(),
        format!("client_name:{}", config.client_name),
        format!("client_instance:{}", config.client_instance_id),
        format!("client_profile:{}", config.client_profile),
    ]
}

#[derive(Clone, Copy)]
enum ClientProfile {
    Balanced,
    DashboardHeavy,
    ReadHeavy,
    WriteHeavy,
}

impl ClientProfile {
    fn from_str(value: &str) -> Self {
        match value {
            "dashboard-heavy" => Self::DashboardHeavy,
            "read-heavy" => Self::ReadHeavy,
            "write-heavy" => Self::WriteHeavy,
            _ => Self::Balanced,
        }
    }

    fn query_distribution(self) -> ClientQueryDistribution {
        match self {
            Self::Balanced => ClientQueryDistribution {
                organizations_list: 3,
                dashboard: 12,
                analytics_overview_24h: 7,
                analytics_overview_1h: 4,
                top_pages: 6,
                hourly_metrics: 6,
                storefront: 27,
                catalog: 25,
                cart_detail: 10,
            },
            Self::DashboardHeavy => ClientQueryDistribution {
                organizations_list: 2,
                dashboard: 34,
                analytics_overview_24h: 12,
                analytics_overview_1h: 8,
                top_pages: 10,
                hourly_metrics: 12,
                storefront: 10,
                catalog: 8,
                cart_detail: 4,
            },
            Self::ReadHeavy => ClientQueryDistribution {
                organizations_list: 3,
                dashboard: 10,
                analytics_overview_24h: 8,
                analytics_overview_1h: 5,
                top_pages: 6,
                hourly_metrics: 6,
                storefront: 31,
                catalog: 24,
                cart_detail: 7,
            },
            Self::WriteHeavy => ClientQueryDistribution {
                organizations_list: 3,
                dashboard: 8,
                analytics_overview_24h: 6,
                analytics_overview_1h: 3,
                top_pages: 5,
                hourly_metrics: 5,
                storefront: 28,
                catalog: 30,
                cart_detail: 12,
            },
        }
    }

    fn write_distribution(self) -> ClientWriteDistribution {
        match self {
            Self::Balanced => ClientWriteDistribution {
                event_ingest: 32,
                cart_create: 28,
                cart_add_item: 22,
                cart_checkout: 18,
            },
            Self::DashboardHeavy => ClientWriteDistribution {
                event_ingest: 45,
                cart_create: 25,
                cart_add_item: 18,
                cart_checkout: 12,
            },
            Self::ReadHeavy => ClientWriteDistribution {
                event_ingest: 30,
                cart_create: 30,
                cart_add_item: 24,
                cart_checkout: 16,
            },
            Self::WriteHeavy => ClientWriteDistribution {
                event_ingest: 24,
                cart_create: 28,
                cart_add_item: 28,
                cart_checkout: 20,
            },
        }
    }

    fn event_distribution(self) -> ClientEventDistribution {
        match self {
            Self::WriteHeavy => ClientEventDistribution {
                page_view: 42,
                click: 24,
                conversion: 14,
                sign_up: 10,
                purchase: 10,
            },
            Self::ReadHeavy => ClientEventDistribution {
                page_view: 62,
                click: 26,
                conversion: 7,
                sign_up: 3,
                purchase: 2,
            },
            _ => ClientEventDistribution::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClientQueryDistribution {
    organizations_list: u32,
    dashboard: u32,
    analytics_overview_24h: u32,
    analytics_overview_1h: u32,
    top_pages: u32,
    hourly_metrics: u32,
    storefront: u32,
    catalog: u32,
    cart_detail: u32,
}

impl ClientQueryDistribution {
    fn total_weight(&self) -> u32 {
        self.organizations_list
            + self.dashboard
            + self.analytics_overview_24h
            + self.analytics_overview_1h
            + self.top_pages
            + self.hourly_metrics
            + self.storefront
            + self.catalog
            + self.cart_detail
    }

    fn select_request_type<R: Rng + ?Sized>(&self, rng: &mut R) -> &'static str {
        let total = self.total_weight().max(1);
        let mut roll = rng.gen_range(0..total);
        for (request_type, weight) in self.weighted_entries() {
            if roll < weight {
                return request_type;
            }
            roll -= weight;
        }
        "dashboard"
    }

    fn weighted_entries(&self) -> [(&'static str, u32); 9] {
        [
            ("organization_list", self.organizations_list),
            ("dashboard", self.dashboard),
            ("analytics_overview_24h", self.analytics_overview_24h),
            ("analytics_overview_1h", self.analytics_overview_1h),
            ("top_pages", self.top_pages),
            ("hourly_metrics", self.hourly_metrics),
            ("storefront", self.storefront),
            ("catalog", self.catalog),
            ("cart_detail", self.cart_detail),
        ]
    }

    fn apply_patch(&mut self, patch: ClientQueryDistributionPatch) {
        if let Some(value) = patch.organizations_list {
            self.organizations_list = value;
        }
        if let Some(value) = patch.dashboard {
            self.dashboard = value;
        }
        if let Some(value) = patch.analytics_overview_24h {
            self.analytics_overview_24h = value;
        }
        if let Some(value) = patch.analytics_overview_1h {
            self.analytics_overview_1h = value;
        }
        if let Some(value) = patch.top_pages {
            self.top_pages = value;
        }
        if let Some(value) = patch.hourly_metrics {
            self.hourly_metrics = value;
        }
        if let Some(value) = patch.storefront {
            self.storefront = value;
        }
        if let Some(value) = patch.catalog {
            self.catalog = value;
        }
        if let Some(value) = patch.cart_detail {
            self.cart_detail = value;
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ClientQueryDistributionPatch {
    organizations_list: Option<u32>,
    dashboard: Option<u32>,
    analytics_overview_24h: Option<u32>,
    analytics_overview_1h: Option<u32>,
    top_pages: Option<u32>,
    hourly_metrics: Option<u32>,
    storefront: Option<u32>,
    catalog: Option<u32>,
    cart_detail: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClientWriteDistribution {
    event_ingest: u32,
    cart_create: u32,
    cart_add_item: u32,
    cart_checkout: u32,
}

impl ClientWriteDistribution {
    fn total_weight(&self) -> u32 {
        self.event_ingest + self.cart_create + self.cart_add_item + self.cart_checkout
    }

    fn select_request_type<R: Rng + ?Sized>(&self, rng: &mut R) -> &'static str {
        let total = self.total_weight().max(1);
        let mut roll = rng.gen_range(0..total);
        for (request_type, weight) in self.weighted_entries() {
            if roll < weight {
                return request_type;
            }
            roll -= weight;
        }
        "event_ingest"
    }

    fn weighted_entries(&self) -> [(&'static str, u32); 4] {
        [
            ("event_ingest", self.event_ingest),
            ("cart_create", self.cart_create),
            ("cart_add_item", self.cart_add_item),
            ("cart_checkout", self.cart_checkout),
        ]
    }

    fn apply_patch(&mut self, patch: ClientWriteDistributionPatch) {
        if let Some(value) = patch.event_ingest {
            self.event_ingest = value;
        }
        if let Some(value) = patch.cart_create {
            self.cart_create = value;
        }
        if let Some(value) = patch.cart_add_item {
            self.cart_add_item = value;
        }
        if let Some(value) = patch.cart_checkout {
            self.cart_checkout = value;
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ClientWriteDistributionPatch {
    event_ingest: Option<u32>,
    cart_create: Option<u32>,
    cart_add_item: Option<u32>,
    cart_checkout: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClientEventDistribution {
    page_view: u32,
    click: u32,
    conversion: u32,
    sign_up: u32,
    purchase: u32,
}

impl Default for ClientEventDistribution {
    fn default() -> Self {
        Self {
            page_view: 55,
            click: 28,
            conversion: 9,
            sign_up: 5,
            purchase: 3,
        }
    }
}

impl ClientEventDistribution {
    fn total_weight(&self) -> u32 {
        self.page_view + self.click + self.conversion + self.sign_up + self.purchase
    }

    fn select_event_type<R: Rng + ?Sized>(&self, rng: &mut R) -> &'static str {
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

    fn apply_patch(&mut self, patch: ClientEventDistributionPatch) {
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
struct ClientEventDistributionPatch {
    page_view: Option<u32>,
    click: Option<u32>,
    conversion: Option<u32>,
    sign_up: Option<u32>,
    purchase: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClientControlSettings {
    queries_per_second: u64,
    events_per_second: u64,
    query_distribution: ClientQueryDistribution,
    write_distribution: ClientWriteDistribution,
    event_distribution: ClientEventDistribution,
}

impl ClientControlSettings {
    fn from_config(config: &TrafficClientConfig) -> Self {
        let profile = config.profile();
        Self {
            queries_per_second: config.queries_per_second,
            events_per_second: config.events_per_second,
            query_distribution: profile.query_distribution(),
            write_distribution: profile.write_distribution(),
            event_distribution: profile.event_distribution(),
        }
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if self.query_distribution.total_weight() == 0 {
            return Err("query_distribution must have at least one non-zero weight".to_string());
        }
        if self.write_distribution.total_weight() == 0 {
            return Err("write_distribution must have at least one non-zero weight".to_string());
        }
        if self.event_distribution.total_weight() == 0 {
            return Err("event_distribution must have at least one non-zero weight".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ClientControlPatch {
    queries_per_second: Option<u64>,
    events_per_second: Option<u64>,
    query_distribution: Option<ClientQueryDistributionPatch>,
    write_distribution: Option<ClientWriteDistributionPatch>,
    event_distribution: Option<ClientEventDistributionPatch>,
}

struct AdaptiveRateLimiter {
    permits: Arc<Semaphore>,
}

impl AdaptiveRateLimiter {
    fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(0)),
        }
    }

    async fn acquire_until(&self, shutdown: &CancellationToken) -> bool {
        tokio::select! {
            result = self.permits.acquire() => {
                let permit = result.expect("client rate limiter semaphore closed");
                permit.forget();
                true
            }
            _ = shutdown.cancelled() => false,
        }
    }

    async fn run_refill_loop<F>(&self, rate_provider: F, shutdown: CancellationToken)
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        let mut carry = 0.0f64;

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.cancelled() => break,
            }
            let rate = rate_provider() as f64;
            let permits_per_tick = rate / 10.0;
            let desired = permits_per_tick + carry;
            let permits_to_add = desired.floor() as usize;
            carry = desired - permits_to_add as f64;

            let desired_capacity = if rate == 0.0 {
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

struct ClientControls {
    settings: RwLock<ClientControlSettings>,
    query_limiter: Arc<AdaptiveRateLimiter>,
    event_limiter: Arc<AdaptiveRateLimiter>,
}

impl ClientControls {
    fn from_config(config: &TrafficClientConfig) -> Arc<Self> {
        Arc::new(Self {
            settings: RwLock::new(ClientControlSettings::from_config(config)),
            query_limiter: Arc::new(AdaptiveRateLimiter::new()),
            event_limiter: Arc::new(AdaptiveRateLimiter::new()),
        })
    }

    fn snapshot(&self) -> ClientControlSettings {
        self.settings
            .read()
            .expect("client controls lock poisoned")
            .clone()
    }

    fn apply_patch(
        &self,
        patch: ClientControlPatch,
    ) -> std::result::Result<ClientControlSettings, String> {
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
        if let Some(distribution_patch) = patch.write_distribution {
            next.write_distribution.apply_patch(distribution_patch);
        }
        if let Some(distribution_patch) = patch.event_distribution {
            next.event_distribution.apply_patch(distribution_patch);
        }

        next.validate()?;
        *self
            .settings
            .write()
            .expect("client controls lock poisoned") = next.clone();
        Ok(next)
    }

    fn start_background_tasks(self: &Arc<Self>, shutdown: CancellationToken) {
        let query_controls = self.clone();
        let query_limiter = self.query_limiter.clone();
        let query_shutdown = shutdown.clone();
        tokio::spawn(async move {
            query_limiter
                .run_refill_loop(
                    move || query_controls.snapshot().queries_per_second,
                    query_shutdown,
                )
                .await;
        });

        let event_controls = self.clone();
        let event_limiter = self.event_limiter.clone();
        let event_shutdown = shutdown.clone();
        tokio::spawn(async move {
            event_limiter
                .run_refill_loop(
                    move || event_controls.snapshot().events_per_second,
                    event_shutdown,
                )
                .await;
        });
    }

    fn query_limiter(&self) -> Arc<AdaptiveRateLimiter> {
        self.query_limiter.clone()
    }

    fn event_limiter(&self) -> Arc<AdaptiveRateLimiter> {
        self.event_limiter.clone()
    }
}

struct ClientOrgCache {
    org_ids: AsyncRwLock<Vec<Uuid>>,
    cart_ids_by_org: AsyncRwLock<std::collections::HashMap<Uuid, Vec<Uuid>>>,
    last_refresh_at: AsyncRwLock<Option<chrono::DateTime<Utc>>>,
}

impl ClientOrgCache {
    fn new() -> Self {
        Self {
            org_ids: AsyncRwLock::new(Vec::new()),
            cart_ids_by_org: AsyncRwLock::new(std::collections::HashMap::new()),
            last_refresh_at: AsyncRwLock::new(None),
        }
    }

    async fn replace(&self, org_ids: Vec<Uuid>) {
        *self.org_ids.write().await = org_ids;
        *self.last_refresh_at.write().await = Some(Utc::now());
    }

    async fn random_org_id(&self) -> Option<Uuid> {
        let org_ids = self.org_ids.read().await;
        if org_ids.is_empty() {
            return None;
        }
        let mut rng = StdRng::from_entropy();
        Some(org_ids[rng.gen_range(0..org_ids.len())])
    }

    async fn len(&self) -> usize {
        self.org_ids.read().await.len()
    }

    async fn last_refresh_at(&self) -> Option<chrono::DateTime<Utc>> {
        *self.last_refresh_at.read().await
    }

    async fn remember_cart(&self, org_id: Uuid, cart_id: Uuid) {
        let mut carts = self.cart_ids_by_org.write().await;
        let entry = carts.entry(org_id).or_default();
        if !entry.contains(&cart_id) {
            entry.push(cart_id);
        }
    }

    async fn forget_cart(&self, org_id: Uuid, cart_id: Uuid) {
        let mut carts = self.cart_ids_by_org.write().await;
        let mut remove_org_entry = false;
        if let Some(entry) = carts.get_mut(&org_id) {
            entry.retain(|known_cart_id| *known_cart_id != cart_id);
            if entry.is_empty() {
                remove_org_entry = true;
            }
        }
        if remove_org_entry {
            carts.remove(&org_id);
        }
    }

    async fn random_cart_id(&self, org_id: Uuid) -> Option<Uuid> {
        let carts = self.cart_ids_by_org.read().await;
        let cart_ids = carts.get(&org_id)?;
        if cart_ids.is_empty() {
            return None;
        }
        let mut rng = StdRng::from_entropy();
        Some(cart_ids[rng.gen_range(0..cart_ids.len())])
    }
}

#[derive(Clone)]
pub struct ClientState {
    config: Arc<TrafficClientConfig>,
    controls: Arc<ClientControls>,
    telemetry: Arc<TelemetryRuntime>,
    http: reqwest::Client,
    org_cache: Arc<ClientOrgCache>,
    shutdown: CancellationToken,
}

#[derive(Serialize)]
struct ClientHealthStatus {
    status: &'static str,
    client_name: String,
    client_instance_id: String,
    client_profile: String,
    target_base_url: String,
    organizations_cached: usize,
    last_organization_refresh_at: Option<chrono::DateTime<Utc>>,
    queries_per_second: u64,
    events_per_second: u64,
}

#[derive(Serialize)]
struct ClientRequestPayload {
    request_type: String,
    endpoint: String,
    http_status: u16,
    response_bytes: usize,
}

#[derive(Serialize)]
struct ClientErrorPayload {
    request_type: String,
    endpoint: String,
    error_message: String,
    http_status: Option<u16>,
}

fn extract_traceparent(request: &Request<Body>) -> Option<String> {
    request
        .headers()
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn inject_traceparent(
    request: reqwest::RequestBuilder,
    span: &TelemetrySpan,
) -> reqwest::RequestBuilder {
    match span.traceparent() {
        Some(traceparent) => request.header("traceparent", traceparent),
        None => request,
    }
}

fn parse_uuid_field(payload_bytes: &[u8], field: &str) -> Option<Uuid> {
    serde_json::from_slice::<Value>(payload_bytes)
        .ok()?
        .get(field)?
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())
}

fn start_outbound_http_span(
    state: &ClientState,
    name: &'static str,
    method: &'static str,
    endpoint: &str,
    url: &str,
    org_id: Option<Uuid>,
) -> TelemetrySpan {
    let mut span = state
        .telemetry
        .tracer()
        .start_span(name, TelemetrySpanKind::Client);
    span.enter();
    span.set_attribute("http.method", method);
    span.set_attribute("http.route", endpoint);
    span.set_attribute("http.url", url);
    span.set_attribute("client.name", &state.config.client_name);
    span.set_attribute("client.instance_id", &state.config.client_instance_id);
    span.set_attribute("client.profile", &state.config.client_profile);
    if let Some(org_id) = org_id {
        span.set_attribute("organization.id", org_id.to_string());
    }
    span
}

pub struct TestClientApp {
    pub router: Router,
    pub state: ClientState,
}

fn build_app(state: ClientState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route(
            "/config",
            get(control_handler).patch(update_control_handler),
        )
        .route(
            "/control",
            get(control_handler).patch(update_control_handler),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            client_http_telemetry_middleware,
        ))
        .with_state(state)
}

pub async fn build_test_client(target_base_url: String) -> AnyResult<TestClientApp> {
    let config = Arc::new(TrafficClientConfig {
        client_name: "traffic-client".to_string(),
        client_instance_id: "test-client-instance".to_string(),
        client_profile: "balanced".to_string(),
        bind_address: "127.0.0.1:0".to_string(),
        target_base_url,
        queries_per_second: 25,
        events_per_second: 10,
        query_workers: 1,
        event_workers: 1,
        organization_fetch_limit: 10,
        organization_refresh_interval_seconds: 3600,
        request_timeout_ms: 3000,
        telemetry_provider: TelemetryProvider::Datadog,
        telemetry_enabled: false,
        telemetry_service: "traffic-client".to_string(),
        telemetry_env: "test".to_string(),
        telemetry_version: "0.0.0-test".to_string(),
        telemetry_site: "datadoghq.com".to_string(),
        telemetry_datadog_api_key: None,
        telemetry_dogstatsd_endpoint: None,
        telemetry_export_interval_seconds: 10,
        telemetry_opentelemetry_endpoint: None,
        telemetry_otlp_export_interval_seconds: 10,
        telemetry_otlp_timeout_seconds: 5,
        telemetry_query_log_every: 1,
        telemetry_event_sample_size: 1,
        telemetry_capture_query_payloads: false,
        telemetry_capture_event_payloads: false,
        telemetry_capture_system_snapshots: false,
    });
    config.validate()?;

    let controls = ClientControls::from_config(&config);
    let shutdown = CancellationToken::new();
    let telemetry = TelemetryRuntime::from_options(config.telemetry_options(), "external-client");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .pool_idle_timeout(Duration::from_secs(30))
        .build()?;

    let state = ClientState {
        config: config.clone(),
        controls,
        telemetry,
        http,
        org_cache: Arc::new(ClientOrgCache::new()),
        shutdown,
    };

    refresh_organizations(&state).await?;

    Ok(TestClientApp {
        router: build_app(state.clone()),
        state,
    })
}

pub async fn execute_single_query_request_for_test(state: &ClientState) -> AnyResult<()> {
    execute_query_request(state).await
}

pub async fn execute_single_event_request_for_test(state: &ClientState) -> AnyResult<()> {
    execute_write_request(state).await
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    init_tracing("traffic_client=info,reqwest=warn");
    install_legacy_telemetry_env_aliases();

    let config = Arc::new(TrafficClientConfig::parse());
    config.validate()?;

    let shutdown = CancellationToken::new();
    let controls = ClientControls::from_config(&config);
    controls.start_background_tasks(shutdown.clone());
    let telemetry = TelemetryRuntime::from_options(config.telemetry_options(), "external-client");
    if telemetry.enabled() {
        telemetry.emit_custom_activity(ActivityEmission {
            descriptor: activity::startup_configuration(),
            org_id: None,
            status: "success",
            latency_us: None,
            error_type: None,
            extra_tags: client_identity_tags(&config),
            payload: &config.startup_payload(),
        });
    }

    let http = reqwest::Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .pool_idle_timeout(Duration::from_secs(30))
        .build()?;

    let state = ClientState {
        config: config.clone(),
        controls: controls.clone(),
        telemetry,
        http,
        org_cache: Arc::new(ClientOrgCache::new()),
        shutdown: shutdown.clone(),
    };

    info!(
        "Starting traffic client '{}' instance '{}' against {}",
        state.config.client_name, state.config.client_instance_id, state.config.target_base_url
    );

    if let Err(error) = refresh_organizations(&state).await {
        warn!("Initial organization refresh failed: {}", error);
    }

    tokio::spawn(start_organization_refresh_loop(state.clone()));
    tokio::spawn(start_query_workers(state.clone()));
    tokio::spawn(start_write_workers(state.clone()));

    let app = build_app(state.clone());

    let listener = tokio::net::TcpListener::bind(&state.config.bind_address).await?;
    info!(
        "Traffic client '{}' instance '{}' listening on http://{}",
        state.config.client_name, state.config.client_instance_id, state.config.bind_address
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown_signal())
        .await?;
    state.shutdown.cancel();
    state.telemetry.shutdown().await;
    Ok(())
}

async fn health_handler(State(state): State<ClientState>) -> Json<ClientHealthStatus> {
    let controls = state.controls.snapshot();
    Json(ClientHealthStatus {
        status: "ok",
        client_name: state.config.client_name.clone(),
        client_instance_id: state.config.client_instance_id.clone(),
        client_profile: state.config.client_profile.clone(),
        target_base_url: state.config.target_base_url.clone(),
        organizations_cached: state.org_cache.len().await,
        last_organization_refresh_at: state.org_cache.last_refresh_at().await,
        queries_per_second: controls.queries_per_second,
        events_per_second: controls.events_per_second,
    })
}

async fn control_handler(State(state): State<ClientState>) -> Json<ClientControlSettings> {
    Json(state.controls.snapshot())
}

async fn update_control_handler(
    State(state): State<ClientState>,
    Json(patch): Json<ClientControlPatch>,
) -> std::result::Result<Json<ClientControlSettings>, (StatusCode, String)> {
    match state.controls.apply_patch(patch) {
        Ok(settings) => {
            state
                .telemetry
                .metrics()
                .record_operation_success("client_runtime_control_update");
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::runtime_control_updated(),
                org_id: None,
                status: "success",
                latency_us: None,
                error_type: None,
                extra_tags: client_identity_tags(&state.config),
                payload: &settings,
            });
            Ok(Json(settings))
        }
        Err(message) => Err((StatusCode::BAD_REQUEST, message)),
    }
}

async fn client_http_telemetry_middleware(
    State(state): State<ClientState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let traceparent = extract_traceparent(&request);
    let matched_path = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or_else(|| request.uri().path())
        .to_string();
    let method = request.method().as_str().to_string();
    let mut span = state.telemetry.tracer().start_span_from_traceparent(
        traceparent.as_deref(),
        "traffic_client.control_http",
        TelemetrySpanKind::Server,
    );
    span.enter();
    span.set_attribute("http.route", &matched_path);
    span.set_attribute("http.method", &method);
    let start = Instant::now();

    let response = next.run(request).await;
    span.set_attribute("http.status_code", response.status().as_u16());
    if response.status().is_server_error() {
        span.record_error("http_error");
    }
    state.telemetry.metrics().record_http_request(
        "client_control",
        &matched_path,
        &method,
        response.status().as_u16(),
        start.elapsed().as_secs_f64(),
    );
    span.finish();
    response
}

async fn start_organization_refresh_loop(state: ClientState) {
    loop {
        tokio::select! {
            _ = sleep(Duration::from_secs(
                state.config.organization_refresh_interval_seconds,
            )) => {}
            _ = state.shutdown.cancelled() => break,
        }

        if let Err(error) = refresh_organizations(&state).await {
            warn!(
                "Organization refresh failed for client '{}': {}",
                state.config.client_name, error
            );
        }
    }
}

async fn refresh_organizations(state: &ClientState) -> AnyResult<usize> {
    let url = format!(
        "{}/api/v1/organizations?limit={}",
        state.config.target_base_url, state.config.organization_fetch_limit
    );
    let endpoint = "/api/v1/organizations";
    let mut span = start_outbound_http_span(
        state,
        "traffic_client.organizations.fetch",
        "GET",
        endpoint,
        &url,
        None,
    );
    span.set_attribute("analytics.request_type", "organization_list");
    let start = Instant::now();

    let response = match inject_traceparent(state.http.get(&url), &span).send().await {
        Ok(response) => response,
        Err(error) => {
            let duration = start.elapsed().as_secs_f64();
            span.record_error("http_error");
            state
                .telemetry
                .metrics()
                .record_operation_error("client_org_refresh", "http_error");
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::query("organization_list"),
                org_id: None,
                status: "error",
                latency_us: Some(duration * 1_000_000.0),
                error_type: Some("http_error"),
                extra_tags: client_identity_tags(&state.config),
                payload: &ClientErrorPayload {
                    request_type: "organization_list".to_string(),
                    endpoint: endpoint.to_string(),
                    error_message: error.to_string(),
                    http_status: None,
                },
            });
            span.finish();
            return Err(error.into());
        }
    };
    let status_code = response.status().as_u16();
    let duration = start.elapsed().as_secs_f64();
    span.set_attribute("http.status_code", status_code);
    state
        .telemetry
        .metrics()
        .record_http_request("client", endpoint, "GET", status_code, duration);

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        state
            .telemetry
            .metrics()
            .record_operation_error("client_org_refresh", "http_error");
        let mut tags = client_identity_tags(&state.config);
        tags.push(format!("http_status:{}", status_code));
        state.telemetry.emit_custom_activity(ActivityEmission {
            descriptor: activity::query("organization_list"),
            org_id: None,
            status: "error",
            latency_us: Some(duration * 1_000_000.0),
            error_type: Some("http_error"),
            extra_tags: tags,
            payload: &ClientErrorPayload {
                request_type: "organization_list".to_string(),
                endpoint: endpoint.to_string(),
                error_message: body,
                http_status: Some(status_code),
            },
        });
        span.record_error("http_error");
        span.finish();
        return Err(anyhow!(
            "organization refresh returned HTTP {}",
            status_code
        ));
    }

    let organizations = response.json::<Vec<Organization>>().await?;
    let org_ids = organizations
        .into_iter()
        .map(|org| org.id)
        .collect::<Vec<_>>();
    let count = org_ids.len();
    state.org_cache.replace(org_ids).await;
    state
        .telemetry
        .metrics()
        .record_operation_success("client_org_refresh");

    let mut tags = client_identity_tags(&state.config);
    tags.push(format!("result_count:{}", count));
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::query("organization_list"),
        org_id: None,
        status: "success",
        latency_us: Some(duration * 1_000_000.0),
        error_type: None,
        extra_tags: tags,
        payload: &json!({
            "source": "server_api",
            "result_count": count,
            "endpoint": endpoint,
        }),
    });
    span.finish();

    Ok(count)
}

async fn start_query_workers(state: ClientState) {
    for worker_id in 0..state.config.query_workers {
        let worker_state = state.clone();
        tokio::spawn(async move {
            loop {
                if !worker_state
                    .controls
                    .query_limiter()
                    .acquire_until(&worker_state.shutdown)
                    .await
                {
                    break;
                }
                if let Err(error) = execute_query_request(&worker_state).await {
                    error!("Query worker {} error: {}", worker_id, error);
                }
            }
        });
    }
}

async fn start_write_workers(state: ClientState) {
    for worker_id in 0..state.config.event_workers {
        let worker_state = state.clone();
        tokio::spawn(async move {
            loop {
                if !worker_state
                    .controls
                    .event_limiter()
                    .acquire_until(&worker_state.shutdown)
                    .await
                {
                    break;
                }
                if let Err(error) = execute_write_request(&worker_state).await {
                    error!("Write worker {} error: {}", worker_id, error);
                }
            }
        });
    }
}

async fn execute_query_request(state: &ClientState) -> AnyResult<()> {
    let mut rng = StdRng::from_entropy();
    let mut request_type = state
        .controls
        .snapshot()
        .query_distribution
        .select_request_type(&mut rng);

    let (endpoint, url, org_id) = match request_type {
        "organization_list" => (
            "/api/v1/organizations".to_string(),
            format!(
                "{}/api/v1/organizations?limit={}",
                state.config.target_base_url,
                state.config.organization_fetch_limit.min(20)
            ),
            None,
        ),
        "dashboard" => {
            let org_id = state
                .org_cache
                .random_org_id()
                .await
                .ok_or_else(|| anyhow!("no organizations cached for dashboard request"))?;
            (
                "/api/v1/organizations/:org_id/dashboard".to_string(),
                format!(
                    "{}/api/v1/organizations/{}/dashboard",
                    state.config.target_base_url, org_id
                ),
                Some(org_id),
            )
        }
        "analytics_overview_1h" => {
            let org_id = state
                .org_cache
                .random_org_id()
                .await
                .ok_or_else(|| anyhow!("no organizations cached for overview request"))?;
            (
                "/api/v1/organizations/:org_id/analytics/overview".to_string(),
                format!(
                    "{}/api/v1/organizations/{}/analytics/overview?hours=1",
                    state.config.target_base_url, org_id
                ),
                Some(org_id),
            )
        }
        "top_pages" => {
            let org_id = state
                .org_cache
                .random_org_id()
                .await
                .ok_or_else(|| anyhow!("no organizations cached for top pages request"))?;
            (
                "/api/v1/organizations/:org_id/analytics/top-pages".to_string(),
                format!(
                    "{}/api/v1/organizations/{}/analytics/top-pages?limit=10",
                    state.config.target_base_url, org_id
                ),
                Some(org_id),
            )
        }
        "hourly_metrics" => {
            let org_id = state
                .org_cache
                .random_org_id()
                .await
                .ok_or_else(|| anyhow!("no organizations cached for hourly metrics request"))?;
            (
                "/api/v1/organizations/:org_id/analytics/hourly".to_string(),
                format!(
                    "{}/api/v1/organizations/{}/analytics/hourly?points=6",
                    state.config.target_base_url, org_id
                ),
                Some(org_id),
            )
        }
        "storefront" => {
            let org_id = state
                .org_cache
                .random_org_id()
                .await
                .ok_or_else(|| anyhow!("no organizations cached for storefront request"))?;
            (
                "/api/v1/organizations/:org_id/storefront".to_string(),
                format!(
                    "{}/api/v1/organizations/{}/storefront",
                    state.config.target_base_url, org_id
                ),
                Some(org_id),
            )
        }
        "catalog" => {
            let org_id = state
                .org_cache
                .random_org_id()
                .await
                .ok_or_else(|| anyhow!("no organizations cached for catalog request"))?;
            (
                "/api/v1/organizations/:org_id/catalog".to_string(),
                format!(
                    "{}/api/v1/organizations/{}/catalog?limit=12",
                    state.config.target_base_url, org_id
                ),
                Some(org_id),
            )
        }
        "cart_detail" => {
            let org_id = state
                .org_cache
                .random_org_id()
                .await
                .ok_or_else(|| anyhow!("no organizations cached for cart request"))?;
            if let Some(cart_id) = state.org_cache.random_cart_id(org_id).await {
                (
                    "/api/v1/organizations/:org_id/carts/:cart_id".to_string(),
                    format!(
                        "{}/api/v1/organizations/{}/carts/{}",
                        state.config.target_base_url, org_id, cart_id
                    ),
                    Some(org_id),
                )
            } else {
                request_type = "storefront";
                (
                    "/api/v1/organizations/:org_id/storefront".to_string(),
                    format!(
                        "{}/api/v1/organizations/{}/storefront",
                        state.config.target_base_url, org_id
                    ),
                    Some(org_id),
                )
            }
        }
        _ => {
            let org_id = state
                .org_cache
                .random_org_id()
                .await
                .ok_or_else(|| anyhow!("no organizations cached for overview request"))?;
            (
                "/api/v1/organizations/:org_id/analytics/overview".to_string(),
                format!(
                    "{}/api/v1/organizations/{}/analytics/overview?hours=24",
                    state.config.target_base_url, org_id
                ),
                Some(org_id),
            )
        }
    };

    let mut span = start_outbound_http_span(
        state,
        "traffic_client.query.request",
        "GET",
        &endpoint,
        &url,
        org_id,
    );
    span.set_attribute("analytics.request_type", request_type);
    let start = Instant::now();
    let response = match inject_traceparent(state.http.get(&url), &span).send().await {
        Ok(response) => response,
        Err(error) => {
            let duration = start.elapsed();
            span.record_error("http_error");
            state
                .telemetry
                .metrics()
                .record_operation_error("client_query_request", "http_error");
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::query(request_type),
                org_id,
                status: "error",
                latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
                error_type: Some("http_error"),
                extra_tags: client_identity_tags(&state.config),
                payload: &ClientErrorPayload {
                    request_type: request_type.to_string(),
                    endpoint: endpoint.clone(),
                    error_message: error.to_string(),
                    http_status: None,
                },
            });
            span.finish();
            return Err(error.into());
        }
    };
    let status_code = response.status().as_u16();
    let duration = start.elapsed();
    span.set_attribute("http.status_code", status_code);
    state
        .telemetry
        .metrics()
        .record_live_latency_ns(duration.as_nanos() as u64);
    state.telemetry.metrics().record_http_request(
        "client",
        &endpoint,
        "GET",
        status_code,
        duration.as_secs_f64(),
    );

    if response.status().is_success() {
        let payload_bytes = response.bytes().await?;
        if request_type == "cart_detail" {
            if let Ok(payload) = serde_json::from_slice::<Value>(&payload_bytes) {
                if let Some(org_id) = org_id {
                    if let Some(cart_id) = payload
                        .get("id")
                        .and_then(Value::as_str)
                        .and_then(|value| Uuid::parse_str(value).ok())
                    {
                        state.org_cache.remember_cart(org_id, cart_id).await;
                    }
                }
            }
        }
        state
            .telemetry
            .metrics()
            .record_operation_success("client_query_request");
        let mut tags = client_identity_tags(&state.config);
        tags.push(format!("http_status:{}", status_code));
        state.telemetry.emit_custom_activity(ActivityEmission {
            descriptor: activity::query(request_type),
            org_id,
            status: "success",
            latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
            error_type: None,
            extra_tags: tags,
            payload: &ClientRequestPayload {
                request_type: request_type.to_string(),
                endpoint,
                http_status: status_code,
                response_bytes: payload_bytes.len(),
            },
        });
        span.finish();
        return Ok(());
    }

    let body = response.text().await.unwrap_or_default();
    state
        .telemetry
        .metrics()
        .record_operation_error("client_query_request", "http_error");
    let mut tags = client_identity_tags(&state.config);
    tags.push(format!("http_status:{}", status_code));
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::query(request_type),
        org_id,
        status: "error",
        latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
        error_type: Some("http_error"),
        extra_tags: tags,
        payload: &ClientErrorPayload {
            request_type: request_type.to_string(),
            endpoint,
            error_message: body,
            http_status: Some(status_code),
        },
    });
    span.record_error("http_error");
    span.finish();
    Err(anyhow!(
        "query request {} returned HTTP {}",
        request_type,
        status_code
    ))
}

async fn execute_write_request(state: &ClientState) -> AnyResult<()> {
    let mut rng = StdRng::from_entropy();
    let write_type = state
        .controls
        .snapshot()
        .write_distribution
        .select_request_type(&mut rng);

    match write_type {
        "cart_create" => execute_cart_create_request(state).await,
        "cart_add_item" => execute_cart_add_item_request(state).await,
        "cart_checkout" => execute_cart_checkout_request(state).await,
        _ => execute_event_ingest_request(state).await,
    }
}

async fn execute_event_ingest_request(state: &ClientState) -> AnyResult<()> {
    let org_id = state
        .org_cache
        .random_org_id()
        .await
        .ok_or_else(|| anyhow!("no organizations cached for event ingest"))?;

    let mut rng = StdRng::from_entropy();
    let event_type = state
        .controls
        .snapshot()
        .event_distribution
        .select_event_type(&mut rng);

    let page_paths = [
        "/dashboard",
        "/analytics",
        "/billing",
        "/users",
        "/reports",
        "/settings",
    ];
    let referrers = [
        "https://www.google.com/",
        "https://www.linkedin.com/",
        "https://news.ycombinator.com/",
        "https://app.partner.example/",
        "https://www.bing.com/",
    ];
    let user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0)",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Mozilla/5.0 (X11; Linux x86_64)",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
    ];
    let plans = ["starter", "pro", "enterprise"];
    let campaigns = ["spring-launch", "retention-q2", "partner-promo"];
    let regions = ["us-east", "eu-west", "ap-southeast"];

    let payload = json!({
        "user_id": Uuid::new_v4(),
        "event_type": event_type,
        "page_url": format!("https://app.example.com{}", page_paths[rng.gen_range(0..page_paths.len())]),
        "referrer": referrers[rng.gen_range(0..referrers.len())],
        "user_agent": user_agents[rng.gen_range(0..user_agents.len())],
        "properties": {
            "client_name": state.config.client_name,
            "client_instance_id": state.config.client_instance_id,
            "client_profile": state.config.client_profile,
            "plan": plans[rng.gen_range(0..plans.len())],
            "campaign": campaigns[rng.gen_range(0..campaigns.len())],
            "region": regions[rng.gen_range(0..regions.len())],
        }
    });

    let endpoint = "/api/v1/organizations/:org_id/events".to_string();
    let url = format!(
        "{}/api/v1/organizations/{}/events",
        state.config.target_base_url, org_id
    );

    let mut span = start_outbound_http_span(
        state,
        "traffic_client.event.request",
        "POST",
        &endpoint,
        &url,
        Some(org_id),
    );
    span.set_attribute("analytics.event_type", event_type);
    let start = Instant::now();
    let response = match inject_traceparent(state.http.post(&url).json(&payload), &span)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let duration = start.elapsed();
            span.record_error("http_error");
            state
                .telemetry
                .metrics()
                .record_operation_error("client_event_request", "http_error");
            let mut tags = client_identity_tags(&state.config);
            tags.push(format!("event_type:{}", event_type));
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::event_ingest(),
                org_id: Some(org_id),
                status: "error",
                latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
                error_type: Some("http_error"),
                extra_tags: tags,
                payload: &ClientErrorPayload {
                    request_type: "event_ingest".to_string(),
                    endpoint: endpoint.clone(),
                    error_message: error.to_string(),
                    http_status: None,
                },
            });
            span.finish();
            return Err(error.into());
        }
    };
    let status_code = response.status().as_u16();
    let duration = start.elapsed();
    span.set_attribute("http.status_code", status_code);
    state
        .telemetry
        .metrics()
        .record_live_latency_ns(duration.as_nanos() as u64);
    state.telemetry.metrics().record_http_request(
        "client",
        &endpoint,
        "POST",
        status_code,
        duration.as_secs_f64(),
    );

    if response.status().is_success() {
        let payload_bytes = response.bytes().await?;
        state.telemetry.metrics().record_event_generated(event_type);
        state
            .telemetry
            .metrics()
            .record_operation_success("client_event_request");
        let mut tags = client_identity_tags(&state.config);
        tags.push(format!("event_type:{}", event_type));
        tags.push(format!("http_status:{}", status_code));
        state.telemetry.emit_custom_activity(ActivityEmission {
            descriptor: activity::event_ingest(),
            org_id: Some(org_id),
            status: "success",
            latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
            error_type: None,
            extra_tags: tags,
            payload: &ClientRequestPayload {
                request_type: "event_ingest".to_string(),
                endpoint,
                http_status: status_code,
                response_bytes: payload_bytes.len(),
            },
        });
        span.finish();
        return Ok(());
    }

    let body = response.text().await.unwrap_or_default();
    state
        .telemetry
        .metrics()
        .record_operation_error("client_event_request", "http_error");
    let mut tags = client_identity_tags(&state.config);
    tags.push(format!("event_type:{}", event_type));
    tags.push(format!("http_status:{}", status_code));
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::event_ingest(),
        org_id: Some(org_id),
        status: "error",
        latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
        error_type: Some("http_error"),
        extra_tags: tags,
        payload: &ClientErrorPayload {
            request_type: "event_ingest".to_string(),
            endpoint,
            error_message: body,
            http_status: Some(status_code),
        },
    });
    span.record_error("http_error");
    span.finish();
    Err(anyhow!("event ingest returned HTTP {}", status_code))
}

async fn execute_cart_create_request(state: &ClientState) -> AnyResult<()> {
    let org_id = state
        .org_cache
        .random_org_id()
        .await
        .ok_or_else(|| anyhow!("no organizations cached for cart creation"))?;
    create_cart_for_org(state, org_id).await.map(|_| ())
}

async fn execute_cart_add_item_request(state: &ClientState) -> AnyResult<()> {
    let org_id = state
        .org_cache
        .random_org_id()
        .await
        .ok_or_else(|| anyhow!("no organizations cached for cart updates"))?;
    let cart_id = match state.org_cache.random_cart_id(org_id).await {
        Some(cart_id) => cart_id,
        None => create_cart_for_org(state, org_id).await?,
    };
    add_item_to_cart_for_org(state, org_id, cart_id)
        .await
        .map(|_| ())
}

async fn execute_cart_checkout_request(state: &ClientState) -> AnyResult<()> {
    let org_id = state
        .org_cache
        .random_org_id()
        .await
        .ok_or_else(|| anyhow!("no organizations cached for checkout"))?;
    let cart_id = match state.org_cache.random_cart_id(org_id).await {
        Some(cart_id) => cart_id,
        None => create_cart_for_org(state, org_id).await?,
    };
    checkout_cart_for_org(state, org_id, cart_id).await
}

async fn create_cart_for_org(state: &ClientState, org_id: Uuid) -> AnyResult<Uuid> {
    let quantity = StdRng::from_entropy().gen_range(1..=3);
    let endpoint = "/api/v1/organizations/:org_id/carts".to_string();
    let url = format!(
        "{}/api/v1/organizations/{}/carts",
        state.config.target_base_url, org_id
    );
    let payload = json!({
        "quantity": quantity,
        "metadata": {
            "client_name": state.config.client_name,
            "client_instance_id": state.config.client_instance_id,
            "client_profile": state.config.client_profile,
            "channel": "traffic-client",
        }
    });

    let mut span = start_outbound_http_span(
        state,
        "traffic_client.cart.create",
        "POST",
        &endpoint,
        &url,
        Some(org_id),
    );
    span.set_attribute("commerce.action", "cart_create");
    span.set_attribute("commerce.quantity", quantity);
    let start = Instant::now();
    let response = match inject_traceparent(state.http.post(&url).json(&payload), &span)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let duration = start.elapsed();
            span.record_error("http_error");
            state
                .telemetry
                .metrics()
                .record_operation_error("client_cart_create", "http_error");
            let mut tags = client_identity_tags(&state.config);
            tags.push("write_type:cart_create".to_string());
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::cart_created(),
                org_id: Some(org_id),
                status: "error",
                latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
                error_type: Some("http_error"),
                extra_tags: tags,
                payload: &ClientErrorPayload {
                    request_type: "cart_create".to_string(),
                    endpoint: endpoint.clone(),
                    error_message: error.to_string(),
                    http_status: None,
                },
            });
            span.finish();
            return Err(error.into());
        }
    };
    let status_code = response.status().as_u16();
    let duration = start.elapsed();
    span.set_attribute("http.status_code", status_code);
    state.telemetry.metrics().record_http_request(
        "client",
        &endpoint,
        "POST",
        status_code,
        duration.as_secs_f64(),
    );

    if response.status().is_success() {
        let payload_bytes = response.bytes().await?;
        let cart_id = parse_uuid_field(&payload_bytes, "cart_id")
            .ok_or_else(|| anyhow!("cart_create response missing cart_id"))?;
        state.org_cache.remember_cart(org_id, cart_id).await;
        state
            .telemetry
            .metrics()
            .record_operation_success("client_cart_create");
        let mut tags = client_identity_tags(&state.config);
        tags.push("write_type:cart_create".to_string());
        tags.push(format!("http_status:{}", status_code));
        tags.push(format!("cart_id:{}", cart_id));
        state.telemetry.emit_custom_activity(ActivityEmission {
            descriptor: activity::cart_created(),
            org_id: Some(org_id),
            status: "success",
            latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
            error_type: None,
            extra_tags: tags,
            payload: &ClientRequestPayload {
                request_type: "cart_create".to_string(),
                endpoint,
                http_status: status_code,
                response_bytes: payload_bytes.len(),
            },
        });
        span.finish();
        return Ok(cart_id);
    }

    let body = response.text().await.unwrap_or_default();
    state
        .telemetry
        .metrics()
        .record_operation_error("client_cart_create", "http_error");
    let mut tags = client_identity_tags(&state.config);
    tags.push("write_type:cart_create".to_string());
    tags.push(format!("http_status:{}", status_code));
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::cart_created(),
        org_id: Some(org_id),
        status: "error",
        latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
        error_type: Some("http_error"),
        extra_tags: tags,
        payload: &ClientErrorPayload {
            request_type: "cart_create".to_string(),
            endpoint,
            error_message: body,
            http_status: Some(status_code),
        },
    });
    span.record_error("http_error");
    span.finish();
    Err(anyhow!("cart create returned HTTP {}", status_code))
}

async fn add_item_to_cart_for_org(
    state: &ClientState,
    org_id: Uuid,
    cart_id: Uuid,
) -> AnyResult<Uuid> {
    let quantity = StdRng::from_entropy().gen_range(1..=2);
    let endpoint = "/api/v1/organizations/:org_id/carts/:cart_id/items".to_string();
    let url = format!(
        "{}/api/v1/organizations/{}/carts/{}/items",
        state.config.target_base_url, org_id, cart_id
    );
    let payload = json!({ "quantity": quantity });

    let mut span = start_outbound_http_span(
        state,
        "traffic_client.cart.add_item",
        "POST",
        &endpoint,
        &url,
        Some(org_id),
    );
    span.set_attribute("cart.id", cart_id.to_string());
    span.set_attribute("commerce.action", "cart_add_item");
    span.set_attribute("commerce.quantity", quantity);
    let start = Instant::now();
    let response = match inject_traceparent(state.http.post(&url).json(&payload), &span)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let duration = start.elapsed();
            span.record_error("http_error");
            state
                .telemetry
                .metrics()
                .record_operation_error("client_cart_add_item", "http_error");
            let mut tags = client_identity_tags(&state.config);
            tags.push("write_type:cart_add_item".to_string());
            tags.push(format!("cart_id:{}", cart_id));
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::cart_item_added(),
                org_id: Some(org_id),
                status: "error",
                latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
                error_type: Some("http_error"),
                extra_tags: tags,
                payload: &ClientErrorPayload {
                    request_type: "cart_add_item".to_string(),
                    endpoint: endpoint.clone(),
                    error_message: error.to_string(),
                    http_status: None,
                },
            });
            span.finish();
            return Err(error.into());
        }
    };
    let status_code = response.status().as_u16();
    let duration = start.elapsed();
    span.set_attribute("http.status_code", status_code);
    state.telemetry.metrics().record_http_request(
        "client",
        &endpoint,
        "POST",
        status_code,
        duration.as_secs_f64(),
    );

    if response.status().is_success() {
        let payload_bytes = response.bytes().await?;
        let returned_cart_id = parse_uuid_field(&payload_bytes, "cart_id").unwrap_or(cart_id);
        state
            .org_cache
            .remember_cart(org_id, returned_cart_id)
            .await;
        state
            .telemetry
            .metrics()
            .record_operation_success("client_cart_add_item");
        let mut tags = client_identity_tags(&state.config);
        tags.push("write_type:cart_add_item".to_string());
        tags.push(format!("http_status:{}", status_code));
        tags.push(format!("cart_id:{}", returned_cart_id));
        state.telemetry.emit_custom_activity(ActivityEmission {
            descriptor: activity::cart_item_added(),
            org_id: Some(org_id),
            status: "success",
            latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
            error_type: None,
            extra_tags: tags,
            payload: &ClientRequestPayload {
                request_type: "cart_add_item".to_string(),
                endpoint,
                http_status: status_code,
                response_bytes: payload_bytes.len(),
            },
        });
        span.finish();
        return Ok(returned_cart_id);
    }

    let body = response.text().await.unwrap_or_default();
    state
        .telemetry
        .metrics()
        .record_operation_error("client_cart_add_item", "http_error");
    let mut tags = client_identity_tags(&state.config);
    tags.push("write_type:cart_add_item".to_string());
    tags.push(format!("http_status:{}", status_code));
    tags.push(format!("cart_id:{}", cart_id));
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::cart_item_added(),
        org_id: Some(org_id),
        status: "error",
        latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
        error_type: Some("http_error"),
        extra_tags: tags,
        payload: &ClientErrorPayload {
            request_type: "cart_add_item".to_string(),
            endpoint,
            error_message: body,
            http_status: Some(status_code),
        },
    });
    span.record_error("http_error");
    span.finish();
    Err(anyhow!("cart add item returned HTTP {}", status_code))
}

async fn checkout_cart_for_org(state: &ClientState, org_id: Uuid, cart_id: Uuid) -> AnyResult<()> {
    let payment_methods = ["credit_card", "paypal", "stripe", "debit_card"];
    let mut rng = StdRng::from_entropy();
    let payment_method = payment_methods[rng.gen_range(0..payment_methods.len())];
    let endpoint = "/api/v1/organizations/:org_id/carts/:cart_id/checkout".to_string();
    let url = format!(
        "{}/api/v1/organizations/{}/carts/{}/checkout",
        state.config.target_base_url, org_id, cart_id
    );
    let payload = json!({ "payment_method": payment_method });

    let mut span = start_outbound_http_span(
        state,
        "traffic_client.cart.checkout",
        "POST",
        &endpoint,
        &url,
        Some(org_id),
    );
    span.set_attribute("cart.id", cart_id.to_string());
    span.set_attribute("commerce.action", "cart_checkout");
    span.set_attribute("commerce.payment_method", payment_method);
    let start = Instant::now();
    let response = match inject_traceparent(state.http.post(&url).json(&payload), &span)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let duration = start.elapsed();
            span.record_error("http_error");
            state
                .telemetry
                .metrics()
                .record_operation_error("client_cart_checkout", "http_error");
            let mut tags = client_identity_tags(&state.config);
            tags.push("write_type:cart_checkout".to_string());
            tags.push(format!("cart_id:{}", cart_id));
            state.telemetry.emit_custom_activity(ActivityEmission {
                descriptor: activity::cart_checked_out(),
                org_id: Some(org_id),
                status: "error",
                latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
                error_type: Some("http_error"),
                extra_tags: tags,
                payload: &ClientErrorPayload {
                    request_type: "cart_checkout".to_string(),
                    endpoint: endpoint.clone(),
                    error_message: error.to_string(),
                    http_status: None,
                },
            });
            span.finish();
            return Err(error.into());
        }
    };
    let status_code = response.status().as_u16();
    let duration = start.elapsed();
    span.set_attribute("http.status_code", status_code);
    state.telemetry.metrics().record_http_request(
        "client",
        &endpoint,
        "POST",
        status_code,
        duration.as_secs_f64(),
    );

    if response.status().is_success() {
        let payload_bytes = response.bytes().await?;
        state.org_cache.forget_cart(org_id, cart_id).await;
        state
            .telemetry
            .metrics()
            .record_operation_success("client_cart_checkout");
        let mut tags = client_identity_tags(&state.config);
        tags.push("write_type:cart_checkout".to_string());
        tags.push(format!("http_status:{}", status_code));
        tags.push(format!("cart_id:{}", cart_id));
        tags.push(format!("payment_method:{}", payment_method));
        state.telemetry.emit_custom_activity(ActivityEmission {
            descriptor: activity::cart_checked_out(),
            org_id: Some(org_id),
            status: "success",
            latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
            error_type: None,
            extra_tags: tags,
            payload: &ClientRequestPayload {
                request_type: "cart_checkout".to_string(),
                endpoint,
                http_status: status_code,
                response_bytes: payload_bytes.len(),
            },
        });
        span.finish();
        return Ok(());
    }

    let body = response.text().await.unwrap_or_default();
    state
        .telemetry
        .metrics()
        .record_operation_error("client_cart_checkout", "http_error");
    let mut tags = client_identity_tags(&state.config);
    tags.push("write_type:cart_checkout".to_string());
    tags.push(format!("http_status:{}", status_code));
    tags.push(format!("cart_id:{}", cart_id));
    state.telemetry.emit_custom_activity(ActivityEmission {
        descriptor: activity::cart_checked_out(),
        org_id: Some(org_id),
        status: "error",
        latency_us: Some(duration.as_secs_f64() * 1_000_000.0),
        error_type: Some("http_error"),
        extra_tags: tags,
        payload: &ClientErrorPayload {
            request_type: "cart_checkout".to_string(),
            endpoint,
            error_message: body,
            http_status: Some(status_code),
        },
    });
    span.record_error("http_error");
    span.finish();
    Err(anyhow!("cart checkout returned HTTP {}", status_code))
}
