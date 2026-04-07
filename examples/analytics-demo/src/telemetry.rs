// Telemetry Runtime
//
// Central provider-neutral surface for telemetry initialization, metrics,
// tracing hooks, and activity exports. The current backend uses the Datadog
// NDJSON emitter, but application code only talks to this module so we can
// later swap in a first-party crate with minimal churn.

use fast_telemetry::{Span as FastSpan, SpanCollector, SpanKind as FastSpanKind, SpanStatus};
use fast_telemetry_export::spans::{spawn as spawn_span_exporter, SpanExportConfig};
use fast_telemetry_export::{
    dogstatsd::{run as run_dogstatsd_export, DogStatsDConfig},
    sweeper::{run as run_stale_series_sweeper, SweepConfig},
};
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;
use std::{
    env,
    net::UdpSocket,
    sync::{Arc, Mutex},
};
use tokio::runtime::Handle;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    activity::ActivityEventDescriptor,
    datadog::DatadogExporter,
    metrics::{AppMetrics, FastTelemetryDogStatsDState},
    runtime_controls::RuntimeControlSettings,
    Config,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryProvider {
    Datadog,
}

impl TelemetryProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Datadog => "datadog",
        }
    }
}

pub fn parse_telemetry_provider(value: &str) -> Result<TelemetryProvider, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "datadog" => Ok(TelemetryProvider::Datadog),
        other => Err(format!(
            "unsupported telemetry provider '{other}'; supported values: datadog"
        )),
    }
}

const LEGACY_TELEMETRY_ENV_ALIASES: &[(&str, &[&str])] = &[
    ("TELEMETRY_ENABLED", &["DATADOG_ENABLED"]),
    ("TELEMETRY_PROVIDER", &["DATADOG_PROVIDER"]),
    ("TELEMETRY_SERVICE", &["DATADOG_SERVICE"]),
    ("TELEMETRY_ENV", &["DATADOG_ENV"]),
    ("TELEMETRY_VERSION", &["DATADOG_VERSION"]),
    ("TELEMETRY_SITE", &["DATADOG_SITE"]),
    (
        "TELEMETRY_DATADOG_API_KEY",
        &["DATADOG_API_KEY", "DD_API_KEY"],
    ),
    (
        "TELEMETRY_DOGSTATSD_ENDPOINT",
        &["DATADOG_DOGSTATSD_ENDPOINT"],
    ),
    (
        "TELEMETRY_OPENTELEMETRY_ENDPOINT",
        &[
            "TELEMETRY_OTLP_TRACES_ENDPOINT",
            "DATADOG_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_ENDPOINT",
        ],
    ),
    (
        "TELEMETRY_EXPORT_INTERVAL_SECONDS",
        &["DATADOG_EXPORT_INTERVAL_SECONDS"],
    ),
    (
        "TELEMETRY_OTLP_EXPORT_INTERVAL_SECONDS",
        &["DATADOG_OTLP_EXPORT_INTERVAL_SECONDS"],
    ),
    (
        "TELEMETRY_OTLP_TIMEOUT_SECONDS",
        &["DATADOG_OTLP_TIMEOUT_SECONDS"],
    ),
    ("TELEMETRY_QUERY_LOG_EVERY", &["DATADOG_QUERY_LOG_EVERY"]),
    (
        "TELEMETRY_EVENT_SAMPLE_SIZE",
        &["DATADOG_EVENT_SAMPLE_SIZE"],
    ),
    (
        "TELEMETRY_CAPTURE_QUERY_PAYLOADS",
        &["DATADOG_CAPTURE_QUERY_PAYLOADS"],
    ),
    (
        "TELEMETRY_CAPTURE_EVENT_PAYLOADS",
        &["DATADOG_CAPTURE_EVENT_PAYLOADS"],
    ),
    (
        "TELEMETRY_CAPTURE_SYSTEM_SNAPSHOTS",
        &["DATADOG_CAPTURE_SYSTEM_SNAPSHOTS"],
    ),
];

pub fn install_legacy_telemetry_env_aliases() {
    for (canonical, legacy_keys) in LEGACY_TELEMETRY_ENV_ALIASES {
        if env::var_os(canonical).is_none() {
            for legacy in *legacy_keys {
                if let Some(value) = env::var_os(legacy) {
                    env::set_var(canonical, value);
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TelemetryOptions {
    pub provider: TelemetryProvider,
    pub enabled: bool,
    pub service: String,
    pub environment: String,
    pub version: String,
    pub site: String,
    pub datadog_api_key: Option<String>,
    pub dogstatsd_endpoint: Option<String>,
    pub dogstatsd_export_interval_seconds: u64,
    pub opentelemetry_endpoint: Option<String>,
    pub otlp_export_interval_seconds: u64,
    pub otlp_export_timeout_seconds: u64,
    pub query_log_every: u64,
    pub event_sample_size: usize,
    pub capture_query_payloads: bool,
    pub capture_event_payloads: bool,
    pub capture_system_snapshots: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventBatchSummary {
    pub operations_per_second: u64,
    pub writes: u64,
    pub reads: u64,
    pub write_ratio: f64,
    pub total_keys: u64,
    pub duration_ms: f64,
    pub event_type_breakdown: Value,
    pub samples: Vec<TelemetrySample>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TelemetrySample {
    pub record_type: String,
    pub event_name: String,
    pub tags: Vec<String>,
    pub status: String,
    pub payload: Value,
}

impl TelemetrySample {
    pub fn new(record_type: &str, payload: Value) -> Self {
        let sanitized = record_type.replace('_', ".");
        Self {
            record_type: record_type.to_string(),
            event_name: format!("analytics.sample.{}", sanitized),
            tags: sample_tags(record_type),
            status: "success".to_string(),
            payload,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheWarmupSummary {
    pub phase: String,
    pub organizations: usize,
    pub keys_written: u64,
    pub duration_seconds: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemSnapshot {
    pub active_organizations: i64,
    pub events_per_second: i64,
    pub queries_per_second: i64,
    pub cache_hit_ratio: f64,
    pub query_count: u64,
    pub avg_latency_us: f64,
    pub p50_latency_us: f64,
    pub p95_latency_us: f64,
    pub p99_latency_us: f64,
}

pub struct ActivityEmission<'a, T> {
    pub descriptor: ActivityEventDescriptor,
    pub org_id: Option<Uuid>,
    pub status: &'a str,
    pub latency_us: Option<f64>,
    pub error_type: Option<&'a str>,
    pub extra_tags: Vec<String>,
    pub payload: &'a T,
}

pub struct SerializedActivityEmission {
    pub descriptor: ActivityEventDescriptor,
    pub org_id: Option<Uuid>,
    pub status: String,
    pub latency_us: Option<f64>,
    pub error_type: Option<String>,
    pub extra_tags: Vec<String>,
    pub payload: Value,
}

pub trait TelemetryBackend: Send + Sync {
    fn enabled(&self) -> bool;
    fn event_sample_size(&self) -> usize;
    fn emit_startup(&self, config: &Config);
    fn emit_cache_warmup(&self, summary: &CacheWarmupSummary);
    fn emit_query_result(
        &self,
        query_type: &str,
        org_id: Uuid,
        cache_hit: bool,
        latency_ns: u64,
        payload: &Value,
    );
    fn emit_query_error(
        &self,
        query_type: &str,
        org_id: Uuid,
        error_type: &str,
        error_message: &str,
        latency_ns: Option<u64>,
    );
    fn emit_event_batch(&self, summary: &EventBatchSummary);
    fn emit_system_snapshot(&self, snapshot: &SystemSnapshot);
    fn emit_runtime_control_update(&self, settings: &RuntimeControlSettings);
    fn emit_custom_activity(&self, activity: SerializedActivityEmission);
    fn shutdown(&self) {}
}

pub struct TelemetryRuntime {
    metrics: Arc<AppMetrics>,
    tracer: TelemetryTracer,
    backend: Arc<dyn TelemetryBackend>,
    cancel: Option<CancellationToken>,
    dogstatsd_flush: Option<DogStatsDFlushHandle>,
}

impl TelemetryRuntime {
    pub fn from_options(options: TelemetryOptions, mode: &str) -> Arc<Self> {
        let metrics = Arc::new(AppMetrics::new());
        let provider = options.provider;
        let cancel = CancellationToken::new();
        let span_collector = options
            .opentelemetry_endpoint
            .as_ref()
            .map(|_| Arc::new(SpanCollector::new(4, 4096)));
        let tracer = TelemetryTracer::new(provider, mode, span_collector.clone());

        let backend: Arc<dyn TelemetryBackend> = match provider {
            TelemetryProvider::Datadog => {
                DatadogExporter::from_options(options.clone(), mode, metrics.clone())
            }
        };
        let mut dogstatsd_flush = None;

        if options.enabled {
            if let Ok(handle) = Handle::try_current() {
                if let Some(endpoint) = options.dogstatsd_endpoint.clone() {
                    dogstatsd_flush = Some(spawn_dogstatsd_pipeline(
                        &handle,
                        metrics.clone(),
                        &options,
                        mode,
                        endpoint,
                        cancel.clone(),
                    ));
                }
            }

            if let (Some(endpoint), Some(collector)) =
                (options.opentelemetry_endpoint.clone(), span_collector)
            {
                spawn_otlp_span_pipeline(collector, &options, mode, endpoint, cancel.clone());
            }
        }

        Arc::new(Self {
            metrics,
            tracer,
            backend,
            cancel: Some(cancel),
            dogstatsd_flush,
        })
    }

    pub fn metrics(&self) -> &Arc<AppMetrics> {
        &self.metrics
    }

    pub fn tracer(&self) -> &TelemetryTracer {
        &self.tracer
    }

    pub fn enabled(&self) -> bool {
        self.backend.enabled()
    }

    pub fn event_sample_size(&self) -> usize {
        self.backend.event_sample_size()
    }

    pub fn emit_startup(&self, config: &Config) {
        self.backend.emit_startup(config);
    }

    pub fn emit_cache_warmup(&self, summary: &CacheWarmupSummary) {
        self.backend.emit_cache_warmup(summary);
    }

    pub fn emit_query_result(
        &self,
        query_type: &str,
        org_id: Uuid,
        cache_hit: bool,
        latency_ns: u64,
        payload: &Value,
    ) {
        self.backend
            .emit_query_result(query_type, org_id, cache_hit, latency_ns, payload);
    }

    pub fn emit_query_error(
        &self,
        query_type: &str,
        org_id: Uuid,
        error_type: &str,
        error_message: &str,
        latency_ns: Option<u64>,
    ) {
        self.backend
            .emit_query_error(query_type, org_id, error_type, error_message, latency_ns);
    }

    pub fn emit_event_batch(&self, summary: &EventBatchSummary) {
        self.backend.emit_event_batch(summary);
    }

    pub fn emit_system_snapshot(&self, snapshot: &SystemSnapshot) {
        self.backend.emit_system_snapshot(snapshot);
    }

    pub fn emit_runtime_control_update(&self, settings: &RuntimeControlSettings) {
        self.backend.emit_runtime_control_update(settings);
    }

    pub fn emit_custom_activity<T>(&self, activity: ActivityEmission<'_, T>)
    where
        T: Serialize,
    {
        self.backend
            .emit_custom_activity(SerializedActivityEmission {
                descriptor: activity.descriptor,
                org_id: activity.org_id,
                status: activity.status.to_string(),
                latency_us: activity.latency_us,
                error_type: activity.error_type.map(str::to_string),
                extra_tags: activity.extra_tags,
                payload: serde_json::to_value(activity.payload).unwrap_or(Value::Null),
            });
    }

    pub async fn shutdown(&self) {
        self.tracer.flush_local();

        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Some(handle) = &self.dogstatsd_flush {
            if let Err(error) = handle.flush(&self.metrics) {
                log::warn!("Failed to flush final DogStatsD metrics: {error}");
            }
        }

        tokio::time::sleep(Duration::from_millis(150)).await;
        self.backend.shutdown();
    }
}

impl Drop for TelemetryRuntime {
    fn drop(&mut self) {
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        self.backend.shutdown();
    }
}

fn spawn_dogstatsd_pipeline(
    handle: &Handle,
    metrics: Arc<AppMetrics>,
    options: &TelemetryOptions,
    mode: &str,
    endpoint: String,
    cancel: CancellationToken,
) -> DogStatsDFlushHandle {
    let export_tags = vec![
        ("service".to_string(), options.service.clone()),
        ("env".to_string(), options.environment.clone()),
        ("version".to_string(), options.version.clone()),
        ("mode".to_string(), mode.to_string()),
        (
            "provider".to_string(),
            options.provider.as_str().to_string(),
        ),
    ];
    let export_interval = Duration::from_secs(options.dogstatsd_export_interval_seconds.max(1));
    let export_metrics = metrics.clone();
    let state = Arc::new(Mutex::new(FastTelemetryDogStatsDState::new()));
    let export_state = state.clone();
    let export_tags_for_task = export_tags.clone();
    let dogstatsd_config = DogStatsDConfig::new(endpoint.clone()).with_interval(export_interval);

    handle.spawn(run_dogstatsd_export(
        dogstatsd_config.clone(),
        cancel.clone(),
        move |output| {
            let mut state = export_state
                .lock()
                .expect("dogstatsd export state lock poisoned");
            let tag_refs = export_tags_for_task
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>();
            export_metrics.export_fast_metrics_dogstatsd_delta(output, &tag_refs, &mut state);
        },
    ));

    handle.spawn(run_stale_series_sweeper(
        SweepConfig::new().with_interval(export_interval),
        cancel,
        move |threshold| metrics.evict_fast_metric_series(threshold),
    ));

    DogStatsDFlushHandle {
        endpoint,
        max_packet_size: dogstatsd_config.max_packet_size,
        export_tags,
        state,
    }
}

fn spawn_otlp_span_pipeline(
    collector: Arc<SpanCollector>,
    options: &TelemetryOptions,
    mode: &str,
    endpoint: String,
    cancel: CancellationToken,
) {
    let config = SpanExportConfig::new(endpoint)
        .with_interval(Duration::from_secs(
            options.otlp_export_interval_seconds.max(1),
        ))
        .with_service_name(options.service.clone())
        .with_scope_name(format!("analytics-demo.{}", normalize_mode_label(mode)))
        .with_timeout(Duration::from_secs(
            options.otlp_export_timeout_seconds.max(1),
        ))
        .with_attribute("deployment.environment", options.environment.clone())
        .with_attribute("service.version", options.version.clone())
        .with_attribute("telemetry.provider", options.provider.as_str())
        .with_attribute("analytics.mode", mode.to_string());
    let config = if let Some(api_key) = options.datadog_api_key.as_ref() {
        config.with_header("dd-api-key", api_key)
    } else {
        config
    };

    let _ = spawn_span_exporter(collector, config, cancel);
}

fn normalize_mode_label(mode: &str) -> String {
    mode.to_ascii_lowercase()
        .replace(' ', "_")
        .replace('+', "plus")
        .replace('-', "_")
}

#[derive(Clone)]
pub struct TelemetryTracer {
    provider: TelemetryProvider,
    mode: String,
    collector: Option<Arc<SpanCollector>>,
}

impl TelemetryTracer {
    fn new(provider: TelemetryProvider, mode: &str, collector: Option<Arc<SpanCollector>>) -> Self {
        Self {
            provider,
            mode: mode.to_string(),
            collector,
        }
    }

    pub fn start_span(&self, name: &'static str, kind: TelemetrySpanKind) -> TelemetrySpan {
        match &self.collector {
            Some(collector) => TelemetrySpan::new(
                self.provider,
                &self.mode,
                name,
                Some(collector.start_span(name, kind.into())),
            ),
            None => TelemetrySpan::new(self.provider, &self.mode, name, None),
        }
    }

    pub fn start_span_from_traceparent(
        &self,
        traceparent: Option<&str>,
        name: &'static str,
        kind: TelemetrySpanKind,
    ) -> TelemetrySpan {
        match &self.collector {
            Some(collector) => TelemetrySpan::new(
                self.provider,
                &self.mode,
                name,
                Some(collector.start_span_from_traceparent(traceparent, name, kind.into())),
            ),
            None => TelemetrySpan::new(self.provider, &self.mode, name, None),
        }
    }

    pub fn flush_local(&self) {
        if let Some(collector) = &self.collector {
            collector.flush_local();
        }
    }
}

#[derive(Clone, Copy)]
pub enum TelemetrySpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl From<TelemetrySpanKind> for FastSpanKind {
    fn from(value: TelemetrySpanKind) -> Self {
        match value {
            TelemetrySpanKind::Internal => FastSpanKind::Internal,
            TelemetrySpanKind::Server => FastSpanKind::Server,
            TelemetrySpanKind::Client => FastSpanKind::Client,
            TelemetrySpanKind::Producer => FastSpanKind::Producer,
            TelemetrySpanKind::Consumer => FastSpanKind::Consumer,
        }
    }
}

pub struct TelemetrySpan {
    provider: TelemetryProvider,
    mode: String,
    name: &'static str,
    inner: Option<FastSpan>,
    error_type: Option<String>,
    finished: bool,
}

impl TelemetrySpan {
    fn new(
        provider: TelemetryProvider,
        mode: &str,
        name: &'static str,
        inner: Option<FastSpan>,
    ) -> Self {
        Self {
            provider,
            mode: mode.to_string(),
            name,
            inner,
            error_type: None,
            finished: false,
        }
    }

    pub fn enter(&mut self) -> &mut Self {
        if let Some(span) = self.inner.as_mut() {
            span.enter();
        }
        self
    }

    pub fn set_attribute(&mut self, key: &str, value: impl ToString) {
        if let Some(span) = self.inner.as_mut() {
            span.set_attribute(key.to_string(), value.to_string());
        }
    }

    pub fn record_error(&mut self, error_type: &str) {
        self.error_type = Some(error_type.to_string());
        if let Some(span) = self.inner.as_mut() {
            span.set_attribute("error.type".to_string(), error_type.to_string());
            span.set_status(SpanStatus::Error {
                message: error_type.to_string().into(),
            });
        }
    }

    #[allow(dead_code)]
    pub fn traceparent(&self) -> Option<String> {
        let span = self.inner.as_ref()?;
        let traceparent = span.traceparent();
        if traceparent.is_empty() {
            None
        } else {
            Some(traceparent)
        }
    }

    pub fn finish(mut self) {
        self.finalize();
    }

    fn finalize(&mut self) {
        if self.finished {
            return;
        }

        if let Some(mut span) = self.inner.take() {
            if self.error_type.is_none() {
                span.set_status(SpanStatus::Ok);
            }
            span.end();
        }

        match self.provider {
            TelemetryProvider::Datadog => {
                let _ = (&self.mode, self.name, &self.error_type);
            }
        }

        self.finished = true;
    }
}

impl Drop for TelemetrySpan {
    fn drop(&mut self) {
        self.finalize();
    }
}

pub fn init_tracing(default_filter: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(default_filter)
        .try_init();
}

pub async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

struct DogStatsDFlushHandle {
    endpoint: String,
    max_packet_size: usize,
    export_tags: Vec<(String, String)>,
    state: Arc<Mutex<FastTelemetryDogStatsDState>>,
}

impl DogStatsDFlushHandle {
    fn flush(&self, metrics: &AppMetrics) -> std::io::Result<()> {
        let mut output = String::with_capacity(16_384);
        {
            let mut state = self
                .state
                .lock()
                .expect("dogstatsd flush state lock poisoned");
            let tag_refs = self
                .export_tags
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>();
            metrics.export_fast_metrics_dogstatsd_delta(&mut output, &tag_refs, &mut state);
        }

        if output.is_empty() {
            return Ok(());
        }

        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.connect(&self.endpoint)?;

        let mut batch = String::with_capacity(self.max_packet_size);
        for line in output.split_inclusive('\n') {
            if line.len() > self.max_packet_size {
                continue;
            }

            if !batch.is_empty() && batch.len() + line.len() > self.max_packet_size {
                socket.send(batch.as_bytes())?;
                batch.clear();
            }

            batch.push_str(line);
        }

        if !batch.is_empty() {
            socket.send(batch.as_bytes())?;
        }

        Ok(())
    }
}

fn sample_tags(record_type: &str) -> Vec<String> {
    let mut tags = vec!["stream:sample".to_string(), "status:success".to_string()];
    let (domain, dataset) = match record_type {
        "session" | "page_view_record" => ("analytics", "sessions"),
        "campaign" | "experiment" | "goal" | "referrer_breakdown" | "funnel_analysis" => {
            ("marketing", "campaigns")
        }
        "product" | "order" | "subscription" | "invoice" | "payment" | "review" => {
            ("commerce", "revenue")
        }
        "cohort_breakdown" => ("analytics", "cohort"),
        "device_breakdown" => ("analytics", "devices"),
        "geo_breakdown" => ("analytics", "geo"),
        _ => ("analytics", "samples"),
    };
    tags.push(format!("domain:{}", domain));
    tags.push(format!("dataset:{}", dataset));
    tags.push(format!("record_type:{}", record_type));
    tags
}

#[cfg(test)]
mod tests {
    use super::{
        install_legacy_telemetry_env_aliases, parse_telemetry_provider, TelemetryProvider,
    };
    use std::ffi::OsString;
    use std::sync::Mutex;

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn parses_supported_provider_names() {
        assert_eq!(
            parse_telemetry_provider("datadog").expect("provider should parse"),
            TelemetryProvider::Datadog
        );
        assert!(parse_telemetry_provider("unknown").is_err());
    }

    #[test]
    fn installs_legacy_datadog_env_aliases() {
        let _guard = ENV_MUTEX.lock().expect("env lock should be available");

        let previous_telemetry = std::env::var_os("TELEMETRY_ENABLED");
        let previous_datadog = std::env::var_os("DATADOG_ENABLED");
        let previous_telemetry_api_key = std::env::var_os("TELEMETRY_DATADOG_API_KEY");
        let previous_dd_api_key = std::env::var_os("DD_API_KEY");
        let previous_telemetry_otel = std::env::var_os("TELEMETRY_OPENTELEMETRY_ENDPOINT");
        let previous_otel = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT");

        std::env::remove_var("TELEMETRY_ENABLED");
        std::env::remove_var("TELEMETRY_DATADOG_API_KEY");
        std::env::remove_var("TELEMETRY_OPENTELEMETRY_ENDPOINT");
        std::env::set_var("DATADOG_ENABLED", "true");
        std::env::set_var("DD_API_KEY", "demo-key");
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel.example:4318");
        install_legacy_telemetry_env_aliases();

        assert_eq!(std::env::var("TELEMETRY_ENABLED").as_deref(), Ok("true"));
        assert_eq!(
            std::env::var("TELEMETRY_DATADOG_API_KEY").as_deref(),
            Ok("demo-key")
        );
        assert_eq!(
            std::env::var("TELEMETRY_OPENTELEMETRY_ENDPOINT").as_deref(),
            Ok("http://otel.example:4318")
        );

        restore_var("TELEMETRY_ENABLED", previous_telemetry);
        restore_var("DATADOG_ENABLED", previous_datadog);
        restore_var("TELEMETRY_DATADOG_API_KEY", previous_telemetry_api_key);
        restore_var("DD_API_KEY", previous_dd_api_key);
        restore_var("TELEMETRY_OPENTELEMETRY_ENDPOINT", previous_telemetry_otel);
        restore_var("OTEL_EXPORTER_OTLP_ENDPOINT", previous_otel);
    }

    fn restore_var(key: &str, value: Option<OsString>) {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}
