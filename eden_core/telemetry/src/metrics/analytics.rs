//! Analytics metrics for sampling and anomaly detection.
//!
//! These metrics provide global aggregates for observability without per-endpoint
//! cardinality explosion.

use crate::labels::{LABEL_ORG_UUID, SYSTEM_ORG_UUID};
use fast_telemetry::{DynamicCounter, DynamicGauge, ExportMetrics};

/// Default shard count for thread-sharded counters.
const SHARD_COUNT: usize = 16;
const SYSTEM_LABELS: &[(&str, &str)] = &[(LABEL_ORG_UUID, SYSTEM_ORG_UUID)];

/// Analytics metrics for sampling and anomaly detection observability.
#[derive(ExportMetrics)]
#[metric_prefix = "analytics"]
#[otlp]
#[clickhouse]
pub struct AnalyticsMetrics {
    // Gauges - global aggregates, emitted per-tick (10s)
    #[help = "Number of active endpoints being tracked"]
    active_endpoints: DynamicGauge,

    #[help = "Current average sampling rate (0-1, stored as percentage 0-100)"]
    sampling_rate: DynamicGauge,

    #[help = "Maximum P95 latency across endpoints in microseconds"]
    latency_p95_us: DynamicGauge,

    #[help = "Maximum P99 latency across endpoints in microseconds"]
    latency_p99_us: DynamicGauge,

    #[help = "Maximum error rate across endpoints (0-100)"]
    error_rate: DynamicGauge,

    // Counters - low-cardinality labels only
    #[help = "Total events sampled for analysis"]
    events_sampled: DynamicCounter,

    #[help = "Events force-sampled due to anomaly conditions"]
    events_force_sampled: DynamicCounter,

    #[help = "Anomaly level transitions"]
    anomaly_transitions: DynamicCounter,

    #[help = "Endpoints evicted due to TTL"]
    endpoints_evicted: DynamicCounter,

    #[help = "Events dropped due to LRU overflow"]
    overflow_events: DynamicCounter,

    #[help = "Currently active burst capture windows"]
    burst_windows_active: DynamicCounter,
}

impl AnalyticsMetrics {
    pub fn new() -> Self {
        Self {
            active_endpoints: DynamicGauge::new(SHARD_COUNT),
            sampling_rate: DynamicGauge::new(SHARD_COUNT),
            latency_p95_us: DynamicGauge::new(SHARD_COUNT),
            latency_p99_us: DynamicGauge::new(SHARD_COUNT),
            error_rate: DynamicGauge::new(SHARD_COUNT),
            events_sampled: DynamicCounter::new(SHARD_COUNT),
            events_force_sampled: DynamicCounter::new(SHARD_COUNT),
            anomaly_transitions: DynamicCounter::new(SHARD_COUNT),
            endpoints_evicted: DynamicCounter::new(SHARD_COUNT),
            overflow_events: DynamicCounter::new(SHARD_COUNT),
            burst_windows_active: DynamicCounter::new(SHARD_COUNT),
        }
    }

    /// Record tick metrics from registry.
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub fn record_tick(
        &self,
        active_endpoints: usize,
        evicted: usize,
        overflow_total: u64,
        sampling_rate: f64,
        max_p95_us: f64,
        max_p99_us: f64,
        max_error_rate: f64,
    ) {
        self.active_endpoints.set(SYSTEM_LABELS, active_endpoints as f64);
        // Store as percentage (0-100) since Gauge uses i64
        self.sampling_rate.set(SYSTEM_LABELS, sampling_rate * 100.0);
        self.latency_p95_us.set(SYSTEM_LABELS, max_p95_us);
        self.latency_p99_us.set(SYSTEM_LABELS, max_p99_us);
        self.error_rate.set(SYSTEM_LABELS, max_error_rate * 100.0);

        if evicted > 0 {
            self.endpoints_evicted.add(SYSTEM_LABELS, evicted as isize);
        }
        if overflow_total > 0 {
            self.overflow_events.add(SYSTEM_LABELS, overflow_total as isize);
        }
    }

    /// Record a sampling event.
    pub fn record_sample(&self) {
        self.events_sampled.inc(SYSTEM_LABELS);
    }

    /// Record a force-sampled event with reason.
    /// Note: reason parameter kept for API compatibility but not used (no labels).
    pub fn record_force_sample(&self, _reason: &str) {
        self.events_force_sampled.inc(SYSTEM_LABELS);
    }

    /// Record an anomaly level transition.
    /// Note: signal/from/to parameters kept for API compatibility but not used (no labels).
    pub fn record_anomaly_transition(&self, _signal: &str, _from_level: &str, _to_level: &str) {
        self.anomaly_transitions.inc(SYSTEM_LABELS);
    }

    /// Record burst window start.
    pub fn record_burst_start(&self) {
        self.burst_windows_active.inc(SYSTEM_LABELS);
    }

    /// Record burst window end.
    pub fn record_burst_end(&self) {
        self.burst_windows_active.add(SYSTEM_LABELS, -1);
    }

    // === Snapshot Methods ===

    pub fn get_active_endpoints(&self) -> i64 {
        self.active_endpoints.get(SYSTEM_LABELS) as i64
    }

    pub fn get_sampling_rate(&self) -> f64 {
        self.sampling_rate.get(SYSTEM_LABELS) / 100.0
    }

    pub fn get_events_sampled(&self) -> u64 {
        self.events_sampled.sum_all() as u64
    }

    pub fn get_burst_windows_active(&self) -> i64 {
        self.burst_windows_active.sum_all() as i64
    }

    /// Runtime series cardinality for the explicit system label set.
    pub fn cardinality(&self) -> usize {
        self.active_endpoints.cardinality()
            + self.sampling_rate.cardinality()
            + self.latency_p95_us.cardinality()
            + self.latency_p99_us.cardinality()
            + self.error_rate.cardinality()
            + self.events_sampled.cardinality()
            + self.events_force_sampled.cardinality()
            + self.anomaly_transitions.cardinality()
            + self.endpoints_evicted.cardinality()
            + self.overflow_events.cardinality()
            + self.burst_windows_active.cardinality()
    }
}

impl Default for AnalyticsMetrics {
    fn default() -> Self {
        Self::new()
    }
}
