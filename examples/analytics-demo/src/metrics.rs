#![allow(dead_code)]

// Prometheus Metrics Collection
//
// Enhanced metrics for monitoring a 10K+ QPS analytics workload with diverse query types

use fast_telemetry::{
    advance_cycle, Counter, DynamicCounter, DynamicGauge, DynamicHistogram, ExportMetrics, Gauge,
    GaugeF64, Histogram,
};
use prometheus::{
    CounterVec, GaugeVec, Histogram as PrometheusHistogram, HistogramOpts, HistogramVec,
    IntCounter, IntGauge, Opts, Registry,
};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::info;

use crate::activity::{KNOWN_ACTIVITY_ERROR_TYPES, KNOWN_ACTIVITY_EVENT_NAMES};

/// Number of slots in the lock-free circular buffer for latency samples
const SAMPLE_SLOTS: usize = 8192;
const FAST_TELEMETRY_MAX_SERIES: usize = 16_384;

/// Lock-free latency histogram using a circular buffer for sampling.
/// Provides accurate percentiles without mutex contention at high QPS.
pub struct LockFreeLatencyHistogram {
    /// Circular buffer of latency samples (in nanoseconds)
    /// Uses AtomicU64 for lock-free writes
    samples: [AtomicU64; SAMPLE_SLOTS],
    /// Write index (wraps around)
    write_idx: AtomicU64,
    /// Total count of all samples seen
    total_count: AtomicU64,
    /// Sum of all latencies in nanoseconds (for average calculation)
    sum_ns: AtomicU64,
    /// Minimum latency seen
    min_ns: AtomicU64,
    /// Maximum latency seen
    max_ns: AtomicU64,
}

impl Default for LockFreeLatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LockFreeLatencyHistogram {
    pub fn new() -> Self {
        Self {
            samples: std::array::from_fn(|_| AtomicU64::new(0)),
            write_idx: AtomicU64::new(0),
            total_count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            min_ns: AtomicU64::new(u64::MAX),
            max_ns: AtomicU64::new(0),
        }
    }

    /// Record a latency sample (lock-free)
    pub fn record(&self, latency_ns: u64) {
        // Increment total count
        self.total_count.fetch_add(1, Ordering::Relaxed);

        // Update sum for average calculation
        self.sum_ns.fetch_add(latency_ns, Ordering::Relaxed);

        // Update min (compare-and-swap loop)
        let mut current_min = self.min_ns.load(Ordering::Relaxed);
        while latency_ns < current_min {
            match self.min_ns.compare_exchange_weak(
                current_min,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // Update max (compare-and-swap loop)
        let mut current_max = self.max_ns.load(Ordering::Relaxed);
        while latency_ns > current_max {
            match self.max_ns.compare_exchange_weak(
                current_max,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }

        // Write to circular buffer (lock-free)
        let idx = self.write_idx.fetch_add(1, Ordering::Relaxed) as usize % SAMPLE_SLOTS;
        self.samples[idx].store(latency_ns, Ordering::Relaxed);
    }

    /// Get percentiles and reset the histogram
    /// Returns (count, avg_us, min_us, max_us, p50_us, p95_us, p99_us)
    pub fn get_percentiles_and_reset(&self) -> (u64, f64, f64, f64, f64, f64, f64) {
        // Snapshot write index and reset it to 0 for fresh start
        let write_idx = self.write_idx.swap(0, Ordering::Relaxed);

        // Snapshot and reset counters
        let count = self.total_count.swap(0, Ordering::Relaxed);
        let sum_ns = self.sum_ns.swap(0, Ordering::Relaxed);
        let min_ns = self.min_ns.swap(u64::MAX, Ordering::Relaxed);
        let max_ns = self.max_ns.swap(0, Ordering::Relaxed);

        if count == 0 {
            return (0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
        }

        // Determine how many valid samples we have
        // If write_idx < SAMPLE_SLOTS, we haven't wrapped yet
        // If write_idx >= SAMPLE_SLOTS, buffer is full
        let sample_count = (write_idx as usize).min(SAMPLE_SLOTS);
        let mut samples: Vec<u64> = Vec::with_capacity(sample_count);

        // Collect and reset samples from circular buffer
        for i in 0..sample_count {
            let val = self.samples[i].swap(0, Ordering::Relaxed);
            if val > 0 {
                samples.push(val);
            }
        }

        if samples.is_empty() {
            return (count, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
        }

        // Sort samples for percentile calculation
        samples.sort_unstable();

        let avg_us = (sum_ns as f64 / count as f64) / 1000.0;
        let min_us = if min_ns == u64::MAX {
            0.0
        } else {
            min_ns as f64 / 1000.0
        };
        let max_us = max_ns as f64 / 1000.0;

        let p50_us = Self::percentile(&samples, 50.0) / 1000.0;
        let p95_us = Self::percentile(&samples, 95.0) / 1000.0;
        let p99_us = Self::percentile(&samples, 99.0) / 1000.0;

        (count, avg_us, min_us, max_us, p50_us, p95_us, p99_us)
    }

    /// Calculate percentile from sorted samples using linear interpolation
    fn percentile(sorted_samples: &[u64], percentile: f64) -> f64 {
        if sorted_samples.is_empty() {
            return 0.0;
        }

        let n = sorted_samples.len();
        let rank = (percentile / 100.0) * (n - 1) as f64;
        let lower_idx = rank.floor() as usize;
        let upper_idx = rank.ceil() as usize;

        if lower_idx == upper_idx || upper_idx >= n {
            return sorted_samples[lower_idx.min(n - 1)] as f64;
        }

        // Linear interpolation between adjacent values
        let fraction = rank - lower_idx as f64;
        let lower_val = sorted_samples[lower_idx] as f64;
        let upper_val = sorted_samples[upper_idx] as f64;

        lower_val + fraction * (upper_val - lower_val)
    }
}

#[derive(ExportMetrics)]
#[metric_prefix = "analytics"]
#[otlp]
pub struct FastTelemetryMetrics {
    #[help = "Total number of events generated"]
    pub events_generated_total: Counter,

    #[help = "Total events by type"]
    pub events_by_type_total: DynamicCounter,

    #[help = "Time spent generating events in microseconds"]
    pub event_generation_duration_us: DynamicHistogram,

    #[help = "Total number of queries executed"]
    pub queries_executed_total: Counter,

    #[help = "Query execution time in microseconds"]
    pub query_duration_us: Histogram,

    #[help = "Query execution time by query type and cache status in microseconds"]
    pub query_duration_by_type_us: DynamicHistogram,

    #[help = "Total analytics queries by query type and cache status"]
    pub queries_by_type_total: DynamicCounter,

    #[help = "Total number of cache hits"]
    pub cache_hits_total: Counter,

    #[help = "Total number of cache misses"]
    pub cache_misses_total: Counter,

    #[help = "Total number of operation errors"]
    pub operation_errors_total: DynamicCounter,

    #[help = "Total number of successful operations"]
    pub operation_success_total: DynamicCounter,

    #[help = "HTTP requests by role, endpoint, method, and status class"]
    pub http_requests_total: DynamicCounter,

    #[help = "HTTP request latency by role, endpoint, and method in microseconds"]
    pub http_request_duration_us: DynamicHistogram,

    #[help = "Cache operation latency in microseconds"]
    pub cache_operation_duration_us: DynamicHistogram,

    #[help = "Database operation latency in microseconds"]
    pub db_operation_duration_us: DynamicHistogram,

    #[help = "Number of active database connections"]
    pub db_connections_active: Gauge,

    #[help = "Database query execution time in microseconds"]
    pub db_query_duration_us: Histogram,

    #[help = "Total number of database queries"]
    pub db_queries_total: Counter,

    #[help = "Total number of Redis operations"]
    pub redis_operations_total: Counter,

    #[help = "Redis operation execution time in microseconds"]
    pub redis_operation_duration_us: Histogram,

    #[help = "Current cache size in bytes"]
    pub cache_size_bytes: Gauge,

    #[help = "Number of active organizations"]
    pub active_organizations: Gauge,

    #[help = "Current events per second rate target"]
    pub events_per_second_current: Gauge,

    #[help = "Current conversion rate percentage"]
    pub conversion_rate_percent: GaugeF64,

    #[help = "Business KPI snapshots exported by the analytics workload"]
    pub business_kpis: DynamicGauge,

    #[help = "Total number of telemetry exports"]
    pub telemetry_exports_total: DynamicCounter,

    #[help = "Total activity events exported for the analytics workload"]
    pub activity_events_total: DynamicCounter,

    #[help = "Latency by activity event name and status in microseconds"]
    pub activity_event_duration_us: DynamicHistogram,

    #[help = "Total activity errors by event name and error type"]
    pub activity_errors_total: DynamicCounter,
}

impl FastTelemetryMetrics {
    pub fn new() -> Self {
        let shards = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(4);

        Self {
            events_generated_total: Counter::new(shards),
            events_by_type_total: DynamicCounter::with_max_series(
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            event_generation_duration_us: DynamicHistogram::with_limits(
                &[
                    10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
                ],
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            queries_executed_total: Counter::new(shards),
            query_duration_us: Histogram::with_latency_buckets(shards),
            query_duration_by_type_us: DynamicHistogram::with_limits(
                &[
                    10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
                ],
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            queries_by_type_total: DynamicCounter::with_max_series(
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            cache_hits_total: Counter::new(shards),
            cache_misses_total: Counter::new(shards),
            operation_errors_total: DynamicCounter::with_max_series(
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            operation_success_total: DynamicCounter::with_max_series(
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            http_requests_total: DynamicCounter::with_max_series(shards, FAST_TELEMETRY_MAX_SERIES),
            http_request_duration_us: DynamicHistogram::with_limits(
                &[
                    10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
                ],
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            cache_operation_duration_us: DynamicHistogram::with_limits(
                &[
                    10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
                ],
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            db_operation_duration_us: DynamicHistogram::with_limits(
                &[
                    10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
                ],
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            db_connections_active: Gauge::new(),
            db_query_duration_us: Histogram::with_latency_buckets(shards),
            db_queries_total: Counter::new(shards),
            redis_operations_total: Counter::new(shards),
            redis_operation_duration_us: Histogram::with_latency_buckets(shards),
            cache_size_bytes: Gauge::new(),
            active_organizations: Gauge::new(),
            events_per_second_current: Gauge::new(),
            conversion_rate_percent: GaugeF64::new(),
            business_kpis: DynamicGauge::with_max_series(shards, FAST_TELEMETRY_MAX_SERIES),
            telemetry_exports_total: DynamicCounter::with_max_series(
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            activity_events_total: DynamicCounter::with_max_series(
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            activity_event_duration_us: DynamicHistogram::with_limits(
                &[
                    10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
                ],
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
            activity_errors_total: DynamicCounter::with_max_series(
                shards,
                FAST_TELEMETRY_MAX_SERIES,
            ),
        }
    }

    pub fn evict_stale_series(&self, max_staleness: u32) -> usize {
        advance_cycle();

        self.events_by_type_total.evict_stale(max_staleness)
            + self.event_generation_duration_us.evict_stale(max_staleness)
            + self.query_duration_by_type_us.evict_stale(max_staleness)
            + self.queries_by_type_total.evict_stale(max_staleness)
            + self.operation_errors_total.evict_stale(max_staleness)
            + self.operation_success_total.evict_stale(max_staleness)
            + self.http_requests_total.evict_stale(max_staleness)
            + self.http_request_duration_us.evict_stale(max_staleness)
            + self.cache_operation_duration_us.evict_stale(max_staleness)
            + self.db_operation_duration_us.evict_stale(max_staleness)
            + self.business_kpis.evict_stale(max_staleness)
            + self.telemetry_exports_total.evict_stale(max_staleness)
            + self.activity_events_total.evict_stale(max_staleness)
            + self.activity_event_duration_us.evict_stale(max_staleness)
            + self.activity_errors_total.evict_stale(max_staleness)
    }
}

impl Default for FastTelemetryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FastTelemetryDogStatsDState {
    inner: FastTelemetryMetricsDogStatsDState,
}

impl FastTelemetryDogStatsDState {
    pub fn new() -> Self {
        Self {
            inner: FastTelemetryMetricsDogStatsDState::new(),
        }
    }
}

impl Default for FastTelemetryDogStatsDState {
    fn default() -> Self {
        Self::new()
    }
}

fn duration_seconds_to_us(duration: f64) -> u64 {
    (duration.max(0.0) * 1_000_000.0).round() as u64
}

/// AppMetrics contains all Prometheus metrics for high-performance monitoring
pub struct AppMetrics {
    pub registry: Registry,
    pub fast_metrics: FastTelemetryMetrics,

    // Event generation metrics
    pub events_generated_total: IntCounter,
    pub events_by_type: CounterVec,
    pub event_generation_duration: PrometheusHistogram,

    // Query execution metrics
    pub queries_executed_total: IntCounter,
    pub query_duration: PrometheusHistogram,
    pub query_duration_by_type: HistogramVec,
    pub queries_by_type_total: CounterVec,
    pub cache_hits_total: IntCounter,
    pub cache_misses_total: IntCounter,

    // Error tracking
    pub operation_errors_total: CounterVec,
    pub operation_success_total: CounterVec,
    pub http_requests_total: CounterVec,
    pub http_request_duration: HistogramVec,

    // Enhanced latency tracking
    pub cache_operation_duration: HistogramVec,
    pub db_operation_duration: HistogramVec,

    // Database performance metrics
    pub db_connections_active: IntGauge,
    pub db_query_duration: PrometheusHistogram,
    pub db_queries_total: IntCounter,

    // Redis cache metrics
    pub redis_operations_total: IntCounter,
    pub redis_operation_duration: PrometheusHistogram,
    pub cache_size_bytes: IntGauge,

    // Business and operational metrics
    pub active_organizations: IntGauge,
    pub events_per_second: IntGauge,
    pub conversion_rate: prometheus::Gauge,
    pub business_kpis: GaugeVec,

    // Export observability
    pub telemetry_exports_total: CounterVec,
    pub activity_events_total: CounterVec,
    pub activity_event_duration: HistogramVec,
    pub activity_errors_total: CounterVec,

    // Live latency tracking with AtomicU64 (values stored in nanoseconds)
    pub live_latency_sum_ns: AtomicU64,
    pub live_latency_count: AtomicU64,
    pub live_latency_min_ns: AtomicU64,
    pub live_latency_max_ns: AtomicU64,

    // Histogram for percentile calculation (p50, p95, p99)
    pub latency_histogram: LockFreeLatencyHistogram,
}

impl AppMetrics {
    /// Create a new metrics registry with all application metrics
    pub fn new() -> Self {
        let registry = Registry::new();

        let events_generated_total =
            IntCounter::new("events_generated_total", "Total number of events generated").unwrap();

        let events_by_type = CounterVec::new(
            Opts::new("events_by_type_total", "Total events by type"),
            &["event_type"],
        )
        .unwrap();

        let event_generation_duration = PrometheusHistogram::with_opts(HistogramOpts::new(
            "event_generation_duration_seconds",
            "Time spent generating events",
        ))
        .unwrap();

        let queries_executed_total =
            IntCounter::new("queries_executed_total", "Total number of queries executed").unwrap();

        let query_duration = PrometheusHistogram::with_opts(HistogramOpts::new(
            "query_duration_seconds",
            "Query execution time",
        ))
        .unwrap();

        let query_duration_by_type = HistogramVec::new(
            HistogramOpts::new(
                "query_duration_by_type_seconds",
                "Query execution time by query type",
            ),
            &["query_type"],
        )
        .unwrap();

        let queries_by_type_total = CounterVec::new(
            Opts::new(
                "queries_by_type_total",
                "Total number of analytics queries by query type and cache status",
            ),
            &["query_type", "cache_status"],
        )
        .unwrap();

        let cache_hits_total =
            IntCounter::new("cache_hits_total", "Total number of cache hits").unwrap();

        let cache_misses_total =
            IntCounter::new("cache_misses_total", "Total number of cache misses").unwrap();

        let operation_errors_total = CounterVec::new(
            Opts::new("operation_errors_total", "Total number of operation errors"),
            &["operation_type", "error_type"],
        )
        .unwrap();

        let operation_success_total = CounterVec::new(
            Opts::new(
                "operation_success_total",
                "Total number of successful operations",
            ),
            &["operation_type"],
        )
        .unwrap();

        let http_requests_total = CounterVec::new(
            Opts::new(
                "http_requests_total",
                "HTTP requests by role, endpoint, method, and status class",
            ),
            &["role", "endpoint", "method", "status_class"],
        )
        .unwrap();

        let http_request_duration = HistogramVec::new(
            HistogramOpts::new(
                "http_request_duration_seconds",
                "HTTP request latency by role, endpoint, and method",
            ),
            &["role", "endpoint", "method"],
        )
        .unwrap();

        let cache_operation_duration = HistogramVec::new(
            HistogramOpts::new(
                "cache_operation_duration_seconds",
                "Cache operation latency",
            ),
            &["operation", "result"],
        )
        .unwrap();

        let db_operation_duration = HistogramVec::new(
            HistogramOpts::new(
                "db_operation_duration_seconds",
                "Database operation latency",
            ),
            &["query_type", "result"],
        )
        .unwrap();

        let db_connections_active = IntGauge::new(
            "db_connections_active",
            "Number of active database connections",
        )
        .unwrap();

        let db_query_duration = PrometheusHistogram::with_opts(HistogramOpts::new(
            "db_query_duration_seconds",
            "Database query execution time",
        ))
        .unwrap();

        let db_queries_total =
            IntCounter::new("db_queries_total", "Total number of database queries").unwrap();

        let redis_operations_total =
            IntCounter::new("redis_operations_total", "Total number of Redis operations").unwrap();

        let redis_operation_duration = PrometheusHistogram::with_opts(HistogramOpts::new(
            "redis_operation_duration_seconds",
            "Redis operation execution time",
        ))
        .unwrap();

        let cache_size_bytes =
            IntGauge::new("cache_size_bytes", "Current cache size in bytes").unwrap();

        let active_organizations =
            IntGauge::new("active_organizations", "Number of active organizations").unwrap();

        let events_per_second = IntGauge::new(
            "events_per_second_current",
            "Current events per second rate",
        )
        .unwrap();

        let conversion_rate = prometheus::Gauge::new(
            "conversion_rate_percent",
            "Current conversion rate percentage",
        )
        .unwrap();

        let business_kpis = GaugeVec::new(
            Opts::new(
                "business_kpis",
                "Business KPI snapshots exported by the analytics workload",
            ),
            &["metric"],
        )
        .unwrap();

        let telemetry_exports_total = CounterVec::new(
            Opts::new(
                "telemetry_exports_total",
                "Total number of Datadog-oriented telemetry exports",
            ),
            &["stream", "status"],
        )
        .unwrap();

        let activity_events_total = CounterVec::new(
            Opts::new(
                "activity_events_total",
                "Total activity events exported for the analytics workload",
            ),
            &["event_name", "status"],
        )
        .unwrap();

        let activity_event_duration = HistogramVec::new(
            HistogramOpts::new(
                "activity_event_duration_seconds",
                "Latency by activity event name and status",
            ),
            &["event_name", "status"],
        )
        .unwrap();

        let activity_errors_total = CounterVec::new(
            Opts::new(
                "activity_errors_total",
                "Total activity errors by event name and error type",
            ),
            &["event_name", "error_type"],
        )
        .unwrap();

        for event_name in KNOWN_ACTIVITY_EVENT_NAMES {
            activity_events_total
                .with_label_values(&[event_name, "success"])
                .inc_by(0.0);
            activity_events_total
                .with_label_values(&[event_name, "error"])
                .inc_by(0.0);
            for error_type in KNOWN_ACTIVITY_ERROR_TYPES {
                activity_errors_total
                    .with_label_values(&[event_name, error_type])
                    .inc_by(0.0);
            }
        }

        // Register all metrics
        registry
            .register(Box::new(events_generated_total.clone()))
            .unwrap();
        registry.register(Box::new(events_by_type.clone())).unwrap();
        registry
            .register(Box::new(event_generation_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(queries_executed_total.clone()))
            .unwrap();
        registry.register(Box::new(query_duration.clone())).unwrap();
        registry
            .register(Box::new(query_duration_by_type.clone()))
            .unwrap();
        registry
            .register(Box::new(queries_by_type_total.clone()))
            .unwrap();
        registry
            .register(Box::new(cache_hits_total.clone()))
            .unwrap();
        registry
            .register(Box::new(cache_misses_total.clone()))
            .unwrap();
        registry
            .register(Box::new(db_connections_active.clone()))
            .unwrap();
        registry
            .register(Box::new(db_query_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(db_queries_total.clone()))
            .unwrap();
        registry
            .register(Box::new(redis_operations_total.clone()))
            .unwrap();
        registry
            .register(Box::new(redis_operation_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(cache_size_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(active_organizations.clone()))
            .unwrap();
        registry
            .register(Box::new(events_per_second.clone()))
            .unwrap();
        registry
            .register(Box::new(conversion_rate.clone()))
            .unwrap();
        registry.register(Box::new(business_kpis.clone())).unwrap();
        registry
            .register(Box::new(operation_errors_total.clone()))
            .unwrap();
        registry
            .register(Box::new(operation_success_total.clone()))
            .unwrap();
        registry
            .register(Box::new(http_requests_total.clone()))
            .unwrap();
        registry
            .register(Box::new(http_request_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(cache_operation_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(db_operation_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(telemetry_exports_total.clone()))
            .unwrap();
        registry
            .register(Box::new(activity_events_total.clone()))
            .unwrap();
        registry
            .register(Box::new(activity_event_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(activity_errors_total.clone()))
            .unwrap();

        Self {
            registry,
            fast_metrics: FastTelemetryMetrics::new(),
            events_generated_total,
            events_by_type,
            event_generation_duration,
            queries_executed_total,
            query_duration,
            query_duration_by_type,
            queries_by_type_total,
            cache_hits_total,
            cache_misses_total,
            operation_errors_total,
            operation_success_total,
            http_requests_total,
            http_request_duration,
            cache_operation_duration,
            db_operation_duration,
            db_connections_active,
            db_query_duration,
            db_queries_total,
            redis_operations_total,
            redis_operation_duration,
            cache_size_bytes,
            active_organizations,
            events_per_second,
            conversion_rate,
            business_kpis,
            telemetry_exports_total,
            activity_events_total,
            activity_event_duration,
            activity_errors_total,
            // Initialize atomic latency trackers
            live_latency_sum_ns: AtomicU64::new(0),
            live_latency_count: AtomicU64::new(0),
            live_latency_min_ns: AtomicU64::new(u64::MAX),
            live_latency_max_ns: AtomicU64::new(0),
            // Initialize histogram for percentiles
            latency_histogram: LockFreeLatencyHistogram::new(),
        }
    }

    pub fn record_event_generated(&self, event_type: &str) {
        self.events_generated_total.inc();
        self.events_by_type.with_label_values(&[event_type]).inc();
        self.fast_metrics.events_generated_total.inc();
        self.fast_metrics
            .events_by_type_total
            .inc(&[("event_type", event_type)]);
    }

    pub fn record_event_generation_duration(&self, duration: f64) {
        self.event_generation_duration.observe(duration);
        self.fast_metrics.event_generation_duration_us.record(
            &[("operation", "event_generation")],
            duration_seconds_to_us(duration),
        );
    }

    pub fn record_query_executed(&self, duration: f64, cache_hit: bool) {
        self.queries_executed_total.inc();
        self.query_duration.observe(duration);
        self.fast_metrics.queries_executed_total.inc();
        self.fast_metrics
            .query_duration_us
            .record(duration_seconds_to_us(duration));

        if cache_hit {
            self.cache_hits_total.inc();
            self.fast_metrics.cache_hits_total.inc();
        } else {
            self.cache_misses_total.inc();
            self.fast_metrics.cache_misses_total.inc();
        }
    }

    pub fn record_query_execution(&self, query_type: &str, duration: f64, cache_hit: bool) {
        let cache_status = if cache_hit { "hit" } else { "miss" };
        self.record_query_executed(duration, cache_hit);
        self.query_duration_by_type
            .with_label_values(&[query_type])
            .observe(duration);
        self.queries_by_type_total
            .with_label_values(&[query_type, cache_status])
            .inc();
        self.fast_metrics.query_duration_by_type_us.record(
            &[("query_type", query_type), ("cache_status", cache_status)],
            duration_seconds_to_us(duration),
        );
        self.fast_metrics
            .queries_by_type_total
            .inc(&[("query_type", query_type), ("cache_status", cache_status)]);
    }

    pub fn record_db_query(&self, duration: f64) {
        self.db_queries_total.inc();
        self.db_query_duration.observe(duration);
        self.fast_metrics.db_queries_total.inc();
        self.fast_metrics
            .db_query_duration_us
            .record(duration_seconds_to_us(duration));
    }

    pub fn record_redis_operation(&self, duration: f64) {
        self.redis_operations_total.inc();
        self.redis_operation_duration.observe(duration);
        self.fast_metrics.redis_operations_total.inc();
        self.fast_metrics
            .redis_operation_duration_us
            .record(duration_seconds_to_us(duration));
    }

    pub fn update_business_metrics(&self, active_orgs: i64, eps: i64, conversion_rate: f64) {
        self.active_organizations.set(active_orgs);
        self.events_per_second.set(eps);
        self.conversion_rate.set(conversion_rate);
        self.fast_metrics.active_organizations.set(active_orgs);
        self.fast_metrics.events_per_second_current.set(eps);
        self.fast_metrics
            .conversion_rate_percent
            .set(conversion_rate);
    }

    pub fn update_business_kpi(&self, metric_name: &str, value: f64) {
        self.business_kpis
            .with_label_values(&[metric_name])
            .set(value);
        self.fast_metrics
            .business_kpis
            .set(&[("metric", metric_name)], value);
    }

    pub fn record_operation_success(&self, operation_type: &str) {
        self.operation_success_total
            .with_label_values(&[operation_type])
            .inc();
        self.fast_metrics
            .operation_success_total
            .inc(&[("operation_type", operation_type)]);
    }

    pub fn record_operation_error(&self, operation_type: &str, error_type: &str) {
        self.operation_errors_total
            .with_label_values(&[operation_type, error_type])
            .inc();
        self.fast_metrics.operation_errors_total.inc(&[
            ("operation_type", operation_type),
            ("error_type", error_type),
        ]);
    }

    pub fn record_http_request(
        &self,
        role: &str,
        endpoint: &str,
        method: &str,
        status_code: u16,
        duration: f64,
    ) {
        let status_class = match status_code {
            100..=199 => "1xx",
            200..=299 => "2xx",
            300..=399 => "3xx",
            400..=499 => "4xx",
            500..=599 => "5xx",
            _ => "other",
        };

        self.http_requests_total
            .with_label_values(&[role, endpoint, method, status_class])
            .inc();
        self.http_request_duration
            .with_label_values(&[role, endpoint, method])
            .observe(duration);
        self.fast_metrics.http_requests_total.inc(&[
            ("role", role),
            ("endpoint", endpoint),
            ("method", method),
            ("status_class", status_class),
        ]);
        self.fast_metrics.http_request_duration_us.record(
            &[("role", role), ("endpoint", endpoint), ("method", method)],
            duration_seconds_to_us(duration),
        );
    }

    pub fn record_cache_operation(&self, operation: &str, result: &str, duration: f64) {
        self.cache_operation_duration
            .with_label_values(&[operation, result])
            .observe(duration);
        self.fast_metrics.cache_operation_duration_us.record(
            &[("operation", operation), ("result", result)],
            duration_seconds_to_us(duration),
        );
    }

    pub fn record_db_operation(&self, query_type: &str, result: &str, duration: f64) {
        self.db_operation_duration
            .with_label_values(&[query_type, result])
            .observe(duration);
        self.fast_metrics.db_operation_duration_us.record(
            &[("query_type", query_type), ("result", result)],
            duration_seconds_to_us(duration),
        );
    }

    pub fn record_telemetry_export(&self, stream: &str, status: &str) {
        self.telemetry_exports_total
            .with_label_values(&[stream, status])
            .inc();
        self.fast_metrics
            .telemetry_exports_total
            .inc(&[("stream", stream), ("status", status)]);
    }

    pub fn record_activity_event(
        &self,
        event_name: &str,
        status: &str,
        duration_seconds: Option<f64>,
        error_type: Option<&str>,
    ) {
        self.activity_events_total
            .with_label_values(&[event_name, status])
            .inc();
        self.fast_metrics
            .activity_events_total
            .inc(&[("event_name", event_name), ("status", status)]);

        if let Some(duration) = duration_seconds {
            self.activity_event_duration
                .with_label_values(&[event_name, status])
                .observe(duration);
            self.fast_metrics.activity_event_duration_us.record(
                &[("event_name", event_name), ("status", status)],
                duration_seconds_to_us(duration),
            );
        }

        if let Some(error_type) = error_type {
            self.activity_errors_total
                .with_label_values(&[event_name, error_type])
                .inc();
            self.fast_metrics
                .activity_errors_total
                .inc(&[("event_name", event_name), ("error_type", error_type)]);
        }
    }

    pub fn set_db_connections_active(&self, value: i64) {
        self.db_connections_active.set(value);
        self.fast_metrics.db_connections_active.set(value);
    }

    pub fn set_active_organizations(&self, value: i64) {
        self.active_organizations.set(value);
        self.fast_metrics.active_organizations.set(value);
    }

    pub fn set_events_per_second_current(&self, value: i64) {
        self.events_per_second.set(value);
        self.fast_metrics.events_per_second_current.set(value);
    }

    pub fn export_fast_metrics_dogstatsd_delta(
        &self,
        output: &mut String,
        tags: &[(&str, &str)],
        state: &mut FastTelemetryDogStatsDState,
    ) {
        self.fast_metrics
            .export_dogstatsd_delta(output, tags, &mut state.inner);
    }

    pub fn evict_fast_metric_series(&self, max_staleness: u32) -> usize {
        self.fast_metrics.evict_stale_series(max_staleness)
    }

    /// Record a request latency (in nanoseconds) using atomic operations
    pub fn record_live_latency_ns(&self, latency_ns: u64) {
        // Record to histogram for percentile calculation
        self.latency_histogram.record(latency_ns);

        // Add to sum
        self.live_latency_sum_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
        // Increment count
        self.live_latency_count.fetch_add(1, Ordering::Relaxed);

        // Update min (compare-and-swap loop)
        let mut current_min = self.live_latency_min_ns.load(Ordering::Relaxed);
        while latency_ns < current_min {
            match self.live_latency_min_ns.compare_exchange_weak(
                current_min,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // Update max (compare-and-swap loop)
        let mut current_max = self.live_latency_max_ns.load(Ordering::Relaxed);
        while latency_ns > current_max {
            match self.live_latency_max_ns.compare_exchange_weak(
                current_max,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }

    /// Get and reset live latency stats, returning (count, avg_us, min_us, max_us)
    pub fn get_and_reset_live_latency(&self) -> (u64, f64, f64, f64) {
        let sum_ns = self.live_latency_sum_ns.swap(0, Ordering::Relaxed);
        let count = self.live_latency_count.swap(0, Ordering::Relaxed);
        let min_ns = self.live_latency_min_ns.swap(u64::MAX, Ordering::Relaxed);
        let max_ns = self.live_latency_max_ns.swap(0, Ordering::Relaxed);

        if count == 0 {
            return (0, 0.0, 0.0, 0.0);
        }

        let avg_us = (sum_ns as f64 / count as f64) / 1000.0;
        let min_us = if min_ns == u64::MAX {
            0.0
        } else {
            min_ns as f64 / 1000.0
        };
        let max_us = max_ns as f64 / 1000.0;

        (count, avg_us, min_us, max_us)
    }

    /// Get and reset percentile latency stats, returning
    /// (count, avg_us, min_us, max_us, p50_us, p95_us, p99_us).
    pub fn take_latency_snapshot(&self) -> (u64, f64, f64, f64, f64, f64, f64) {
        let snapshot = self.latency_histogram.get_percentiles_and_reset();

        // Keep the simple counters in sync with the sampled histogram reset.
        self.live_latency_sum_ns.swap(0, Ordering::Relaxed);
        self.live_latency_count.swap(0, Ordering::Relaxed);
        self.live_latency_min_ns.swap(u64::MAX, Ordering::Relaxed);
        self.live_latency_max_ns.swap(0, Ordering::Relaxed);

        snapshot
    }

    /// Log live latency stats with percentiles
    pub fn log_live_latency(&self) {
        let (count, avg_us, min_us, max_us, p50_us, p95_us, p99_us) = self.take_latency_snapshot();

        if count > 0 {
            info!(
                "Live latency: {} reqs | avg: {:.1}µs | p50: {:.1}µs | p95: {:.1}µs | p99: {:.1}µs | min: {:.1}µs | max: {:.1}µs",
                count, avg_us, p50_us, p95_us, p99_us, min_us, max_us
            );
        }
    }
}

impl Default for AppMetrics {
    fn default() -> Self {
        Self::new()
    }
}
