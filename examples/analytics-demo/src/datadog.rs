// Datadog Export Helpers
//
// Emits structured NDJSON payloads to stdout. The Datadog agent can tail the
// container logs and forward them alongside scraped OpenMetrics data.

use chrono::Utc;
use serde_json::{json, Value};
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

use crate::{
    activity,
    metrics::AppMetrics,
    runtime_controls::RuntimeControlSettings,
    telemetry::{
        CacheWarmupSummary, EventBatchSummary, SerializedActivityEmission, SystemSnapshot,
        TelemetryBackend, TelemetryOptions,
    },
    Config,
};

pub(crate) struct DatadogExporter {
    enabled: bool,
    service: String,
    environment: String,
    version: String,
    site: String,
    mode: String,
    datadog_api_key_configured: bool,
    dogstatsd_endpoint: Option<String>,
    opentelemetry_endpoint: Option<String>,
    query_log_every: u64,
    event_sample_size: usize,
    capture_query_payloads: bool,
    capture_event_payloads: bool,
    capture_system_snapshots: bool,
    query_counter: AtomicU64,
    metrics: Arc<AppMetrics>,
}

impl DatadogExporter {
    pub(crate) fn from_options(
        options: TelemetryOptions,
        mode: &str,
        metrics: Arc<AppMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            enabled: options.enabled,
            service: options.service,
            environment: options.environment,
            version: options.version,
            site: options.site,
            mode: mode.to_string(),
            datadog_api_key_configured: options.datadog_api_key.is_some(),
            dogstatsd_endpoint: options.dogstatsd_endpoint,
            opentelemetry_endpoint: options.opentelemetry_endpoint,
            query_log_every: options.query_log_every.max(1),
            event_sample_size: options.event_sample_size.max(1),
            capture_query_payloads: options.capture_query_payloads,
            capture_event_payloads: options.capture_event_payloads,
            capture_system_snapshots: options.capture_system_snapshots,
            query_counter: AtomicU64::new(0),
            metrics,
        })
    }

    fn emit_activity(&self, activity: SerializedActivityEmission) {
        let SerializedActivityEmission {
            descriptor,
            org_id,
            status,
            latency_us,
            error_type,
            extra_tags,
            payload,
        } = activity;

        self.metrics.record_activity_event(
            descriptor.event_name,
            &status,
            latency_us.map(|value| value / 1_000_000.0),
            error_type.as_deref(),
        );

        let tags = build_tags(
            descriptor.tags,
            &self.service,
            &self.environment,
            &self.mode,
            &status,
            extra_tags,
        );
        let envelope = json!({
            "timestamp": Utc::now().to_rfc3339(),
            "stream": descriptor.stream,
            "event_name": descriptor.event_name,
            "status": status,
            "service": self.service,
            "environment": self.environment,
            "version": self.version,
            "site": self.site,
            "mode": self.mode,
            "organization_id": org_id.map(|id| id.to_string()),
            "tags": tags,
            "ddtags": tags.join(","),
            "latency_us": latency_us,
            "error_type": error_type,
            "payload": payload,
        });

        match serde_json::to_string(&envelope) {
            Ok(line) => {
                let mut stdout = io::stdout().lock();
                match writeln!(stdout, "{}", line) {
                    Ok(_) => {
                        self.metrics
                            .record_telemetry_export(descriptor.stream, "success");
                    }
                    Err(_) => {
                        self.metrics
                            .record_telemetry_export(descriptor.stream, "error");
                        self.metrics.record_activity_event(
                            descriptor.event_name,
                            "error",
                            None,
                            Some("telemetry_export_error"),
                        );
                    }
                }
            }
            Err(_) => {
                self.metrics
                    .record_telemetry_export(descriptor.stream, "error");
                self.metrics.record_activity_event(
                    descriptor.event_name,
                    "error",
                    None,
                    Some("telemetry_export_error"),
                );
            }
        }
    }
}

impl TelemetryBackend for DatadogExporter {
    fn enabled(&self) -> bool {
        self.enabled
    }

    fn event_sample_size(&self) -> usize {
        self.event_sample_size
    }

    fn emit_startup(&self, config: &Config) {
        if !self.enabled {
            return;
        }

        self.emit_activity(SerializedActivityEmission {
            descriptor: activity::startup_configuration(),
            org_id: None,
            status: "success".to_string(),
            latency_us: None,
            error_type: None,
            extra_tags: vec!["lifecycle:start".to_string()],
            payload: json!({
                "redis_enabled": config.redis_enabled,
                "redis_url": config.redis_url,
                "postgres_enabled": config.postgres_enabled,
                "postgres_host": config.postgres_host,
                "postgres_port": config.postgres_port,
                "postgres_database": config.postgres_database,
                "bind_address": config.bind_address,
                "events_per_second": config.events_per_second,
                "queries_per_second": config.queries_per_second,
                "internal_workload_enabled": config.internal_workload_enabled,
                "organizations": config.organizations,
                "users_per_org": config.users_per_org,
                "cache_hit_target": config.cache_hit_target,
                "max_workers": config.max_workers,
                "time_buckets": config.time_buckets,
                "telemetry_datadog_api_key_configured": self.datadog_api_key_configured,
                "telemetry_dogstatsd_endpoint": self.dogstatsd_endpoint,
                "telemetry_opentelemetry_endpoint": self.opentelemetry_endpoint,
            }),
        });
    }

    fn emit_cache_warmup(&self, summary: &CacheWarmupSummary) {
        if !self.enabled {
            return;
        }

        self.emit_activity(SerializedActivityEmission {
            descriptor: activity::cache_warmup(&summary.phase),
            org_id: None,
            status: "success".to_string(),
            latency_us: Some(summary.duration_seconds * 1_000_000.0),
            error_type: None,
            extra_tags: vec![format!("organization_count:{}", summary.organizations)],
            payload: serde_json::to_value(summary).unwrap_or(Value::Null),
        });
    }

    fn emit_query_result(
        &self,
        query_type: &str,
        org_id: Uuid,
        cache_hit: bool,
        latency_ns: u64,
        payload: &Value,
    ) {
        if !self.enabled || !self.capture_query_payloads {
            return;
        }

        let query_idx = self.query_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if cache_hit && !query_idx.is_multiple_of(self.query_log_every) {
            return;
        }

        let latency_us = latency_ns as f64 / 1000.0;
        self.emit_activity(SerializedActivityEmission {
            descriptor: activity::query(query_type),
            org_id: Some(org_id),
            status: "success".to_string(),
            latency_us: Some(latency_us),
            error_type: None,
            extra_tags: vec![
                format!("cache_status:{}", if cache_hit { "hit" } else { "miss" }),
                format!("latency_tier:{}", latency_tier(latency_us)),
            ],
            payload: json!({
                "cache_hit": cache_hit,
                "latency_us": latency_us,
                "payload": payload,
            }),
        });
    }

    fn emit_query_error(
        &self,
        query_type: &str,
        org_id: Uuid,
        error_type: &str,
        error_message: &str,
        latency_ns: Option<u64>,
    ) {
        if !self.enabled {
            return;
        }

        let latency_us = latency_ns.map(|value| value as f64 / 1000.0);
        let mut tags = vec![format!("error_type:{}", error_type)];
        if let Some(latency) = latency_us {
            tags.push(format!("latency_tier:{}", latency_tier(latency)));
        }

        self.emit_activity(SerializedActivityEmission {
            descriptor: activity::query(query_type),
            org_id: Some(org_id),
            status: "error".to_string(),
            latency_us,
            error_type: Some(error_type.to_string()),
            extra_tags: tags,
            payload: json!({
                "error_message": error_message,
                "query_type": query_type,
            }),
        });
    }

    fn emit_event_batch(&self, summary: &EventBatchSummary) {
        if !self.enabled {
            return;
        }

        let payload = if self.capture_event_payloads {
            json!(summary)
        } else {
            json!({
                "operations_per_second": summary.operations_per_second,
                "writes": summary.writes,
                "reads": summary.reads,
                "write_ratio": summary.write_ratio,
                "total_keys": summary.total_keys,
                "duration_ms": summary.duration_ms,
                "event_type_breakdown": summary.event_type_breakdown,
            })
        };

        self.emit_activity(SerializedActivityEmission {
            descriptor: activity::event_batch(),
            org_id: None,
            status: "success".to_string(),
            latency_us: Some(summary.duration_ms * 1000.0),
            error_type: None,
            extra_tags: vec![
                format!(
                    "write_ratio_bucket:{}",
                    write_ratio_bucket(summary.write_ratio)
                ),
                format!("sample_count:{}", summary.samples.len()),
            ],
            payload,
        });
    }

    fn emit_system_snapshot(&self, snapshot: &SystemSnapshot) {
        if !self.enabled || !self.capture_system_snapshots {
            return;
        }

        self.emit_activity(SerializedActivityEmission {
            descriptor: activity::system_snapshot(),
            org_id: None,
            status: "success".to_string(),
            latency_us: Some(snapshot.p95_latency_us.max(snapshot.avg_latency_us)),
            error_type: None,
            extra_tags: vec![
                format!("latency_tier:{}", latency_tier(snapshot.p95_latency_us)),
                format!(
                    "cache_health:{}",
                    cache_hit_ratio_tier(snapshot.cache_hit_ratio)
                ),
            ],
            payload: serde_json::to_value(snapshot).unwrap_or(Value::Null),
        });
    }

    fn emit_runtime_control_update(&self, settings: &RuntimeControlSettings) {
        if !self.enabled {
            return;
        }

        self.emit_activity(SerializedActivityEmission {
            descriptor: activity::runtime_control_updated(),
            org_id: None,
            status: "success".to_string(),
            latency_us: None,
            error_type: None,
            extra_tags: vec![
                format!("queries_per_second:{}", settings.queries_per_second),
                format!("events_per_second:{}", settings.events_per_second),
            ],
            payload: serde_json::to_value(settings).unwrap_or(Value::Null),
        });
    }

    fn emit_custom_activity(&self, activity: SerializedActivityEmission) {
        if !self.enabled {
            return;
        }

        self.emit_activity(activity);
    }
}

fn build_tags(
    base_tags: &[&str],
    service: &str,
    environment: &str,
    mode: &str,
    status: &str,
    extra_tags: Vec<String>,
) -> Vec<String> {
    let mut tags = base_tags
        .iter()
        .map(|tag| (*tag).to_string())
        .collect::<Vec<_>>();
    tags.push(format!("service:{}", service));
    tags.push(format!("env:{}", environment));
    tags.push(format!("mode:{}", mode));
    tags.push(format!("status:{}", status));
    tags.extend(extra_tags);
    tags
}

fn latency_tier(latency_us: f64) -> &'static str {
    if latency_us < 500.0 {
        "sub_millisecond"
    } else if latency_us < 5_000.0 {
        "fast"
    } else if latency_us < 50_000.0 {
        "moderate"
    } else {
        "slow"
    }
}

fn write_ratio_bucket(write_ratio: f64) -> &'static str {
    if write_ratio >= 0.5 {
        "write_heavy"
    } else if write_ratio >= 0.1 {
        "mixed"
    } else {
        "read_heavy"
    }
}

fn cache_hit_ratio_tier(hit_ratio: f64) -> &'static str {
    if hit_ratio >= 0.95 {
        "excellent"
    } else if hit_ratio >= 0.85 {
        "good"
    } else if hit_ratio >= 0.70 {
        "watch"
    } else {
        "poor"
    }
}
