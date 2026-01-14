// Prometheus Metrics Collection
//
// Enhanced metrics for monitoring 10K+ QPS analytics demo with diverse query types

use prometheus::{CounterVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntGauge, Opts, Registry};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::info;

/// Number of slots in the lock-free circular buffer for latency samples
const SAMPLE_SLOTS: usize = 8192;

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

impl LockFreeLatencyHistogram {
    pub fn new() -> Self {
        const ZERO: AtomicU64 = AtomicU64::new(0);
        Self {
            samples: [ZERO; SAMPLE_SLOTS],
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
        // Snapshot and reset counters
        let count = self.total_count.swap(0, Ordering::Relaxed);
        let sum_ns = self.sum_ns.swap(0, Ordering::Relaxed);
        let min_ns = self.min_ns.swap(u64::MAX, Ordering::Relaxed);
        let max_ns = self.max_ns.swap(0, Ordering::Relaxed);

        if count == 0 {
            return (0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
        }

        // Collect samples from circular buffer
        let sample_count = (count as usize).min(SAMPLE_SLOTS);
        let mut samples: Vec<u64> = Vec::with_capacity(sample_count);

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
        let min_us = if min_ns == u64::MAX { 0.0 } else { min_ns as f64 / 1000.0 };
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

/// AppMetrics contains all Prometheus metrics for high-performance monitoring
pub struct AppMetrics {
    pub registry: Registry,

    // Event generation metrics
    pub events_generated_total: IntCounter,
    pub events_by_type: CounterVec,
    pub event_generation_duration: Histogram,

    // Query execution metrics
    pub queries_executed_total: IntCounter,
    pub query_duration: Histogram,
    pub cache_hits_total: IntCounter,
    pub cache_misses_total: IntCounter,

    // Error tracking
    pub operation_errors_total: CounterVec,
    pub operation_success_total: CounterVec,

    // Enhanced latency tracking
    pub cache_operation_duration: HistogramVec,
    pub db_operation_duration: HistogramVec,

    // Database performance metrics
    pub db_connections_active: IntGauge,
    pub db_query_duration: Histogram,
    pub db_queries_total: IntCounter,

    // Redis cache metrics
    pub redis_operations_total: IntCounter,
    pub redis_operation_duration: Histogram,
    pub cache_size_bytes: IntGauge,

    // Business and operational metrics
    pub active_organizations: IntGauge,
    pub events_per_second: IntGauge,
    pub conversion_rate: prometheus::Gauge,

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

        let events_generated_total = IntCounter::new(
            "events_generated_total",
            "Total number of events generated"
        ).unwrap();

        let events_by_type = CounterVec::new(
            Opts::new("events_by_type_total", "Total events by type"),
            &["event_type"]
        ).unwrap();

        let event_generation_duration = Histogram::with_opts(
            HistogramOpts::new(
                "event_generation_duration_seconds",
                "Time spent generating events"
            )
        ).unwrap();

        let queries_executed_total = IntCounter::new(
            "queries_executed_total",
            "Total number of queries executed"
        ).unwrap();

        let query_duration = Histogram::with_opts(
            HistogramOpts::new(
                "query_duration_seconds",
                "Query execution time"
            )
        ).unwrap();

        let cache_hits_total = IntCounter::new(
            "cache_hits_total",
            "Total number of cache hits"
        ).unwrap();

        let cache_misses_total = IntCounter::new(
            "cache_misses_total",
            "Total number of cache misses"
        ).unwrap();

        let operation_errors_total = CounterVec::new(
            Opts::new("operation_errors_total", "Total number of operation errors"),
            &["operation_type", "error_type"]
        ).unwrap();

        let operation_success_total = CounterVec::new(
            Opts::new("operation_success_total", "Total number of successful operations"),
            &["operation_type"]
        ).unwrap();

        let cache_operation_duration = HistogramVec::new(
            HistogramOpts::new("cache_operation_duration_seconds", "Cache operation latency"),
            &["operation", "result"]
        ).unwrap();

        let db_operation_duration = HistogramVec::new(
            HistogramOpts::new("db_operation_duration_seconds", "Database operation latency"),
            &["query_type", "result"]
        ).unwrap();

        let db_connections_active = IntGauge::new(
            "db_connections_active",
            "Number of active database connections"
        ).unwrap();

        let db_query_duration = Histogram::with_opts(
            HistogramOpts::new(
                "db_query_duration_seconds",
                "Database query execution time"
            )
        ).unwrap();

        let db_queries_total = IntCounter::new(
            "db_queries_total",
            "Total number of database queries"
        ).unwrap();

        let redis_operations_total = IntCounter::new(
            "redis_operations_total",
            "Total number of Redis operations"
        ).unwrap();

        let redis_operation_duration = Histogram::with_opts(
            HistogramOpts::new(
                "redis_operation_duration_seconds",
                "Redis operation execution time"
            )
        ).unwrap();

        let cache_size_bytes = IntGauge::new(
            "cache_size_bytes",
            "Current cache size in bytes"
        ).unwrap();

        let active_organizations = IntGauge::new(
            "active_organizations",
            "Number of active organizations"
        ).unwrap();

        let events_per_second = IntGauge::new(
            "events_per_second_current",
            "Current events per second rate"
        ).unwrap();

        let conversion_rate = prometheus::Gauge::new(
            "conversion_rate_percent",
            "Current conversion rate percentage"
        ).unwrap();

        // Register all metrics
        registry.register(Box::new(events_generated_total.clone())).unwrap();
        registry.register(Box::new(events_by_type.clone())).unwrap();
        registry.register(Box::new(event_generation_duration.clone())).unwrap();
        registry.register(Box::new(queries_executed_total.clone())).unwrap();
        registry.register(Box::new(query_duration.clone())).unwrap();
        registry.register(Box::new(cache_hits_total.clone())).unwrap();
        registry.register(Box::new(cache_misses_total.clone())).unwrap();
        registry.register(Box::new(db_connections_active.clone())).unwrap();
        registry.register(Box::new(db_query_duration.clone())).unwrap();
        registry.register(Box::new(db_queries_total.clone())).unwrap();
        registry.register(Box::new(redis_operations_total.clone())).unwrap();
        registry.register(Box::new(redis_operation_duration.clone())).unwrap();
        registry.register(Box::new(cache_size_bytes.clone())).unwrap();
        registry.register(Box::new(active_organizations.clone())).unwrap();
        registry.register(Box::new(events_per_second.clone())).unwrap();
        registry.register(Box::new(conversion_rate.clone())).unwrap();
        registry.register(Box::new(operation_errors_total.clone())).unwrap();
        registry.register(Box::new(operation_success_total.clone())).unwrap();
        registry.register(Box::new(cache_operation_duration.clone())).unwrap();
        registry.register(Box::new(db_operation_duration.clone())).unwrap();

        Self {
            registry,
            events_generated_total,
            events_by_type,
            event_generation_duration,
            queries_executed_total,
            query_duration,
            cache_hits_total,
            cache_misses_total,
            operation_errors_total,
            operation_success_total,
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
    }

    pub fn record_query_executed(&self, duration: f64, cache_hit: bool) {
        self.queries_executed_total.inc();
        self.query_duration.observe(duration);

        if cache_hit {
            self.cache_hits_total.inc();
        } else {
            self.cache_misses_total.inc();
        }
    }

    pub fn record_db_query(&self, duration: f64) {
        self.db_queries_total.inc();
        self.db_query_duration.observe(duration);
    }

    pub fn record_redis_operation(&self, duration: f64) {
        self.redis_operations_total.inc();
        self.redis_operation_duration.observe(duration);
    }

    pub fn update_business_metrics(&self, active_orgs: i64, eps: i64, conversion_rate: f64) {
        self.active_organizations.set(active_orgs);
        self.events_per_second.set(eps);
        self.conversion_rate.set(conversion_rate);
    }

    pub fn record_operation_success(&self, operation_type: &str) {
        self.operation_success_total.with_label_values(&[operation_type]).inc();
    }

    pub fn record_operation_error(&self, operation_type: &str, error_type: &str) {
        self.operation_errors_total.with_label_values(&[operation_type, error_type]).inc();
    }

    pub fn record_cache_operation(&self, operation: &str, result: &str, duration: f64) {
        self.cache_operation_duration.with_label_values(&[operation, result]).observe(duration);
    }

    pub fn record_db_operation(&self, query_type: &str, result: &str, duration: f64) {
        self.db_operation_duration.with_label_values(&[query_type, result]).observe(duration);
    }

    /// Record a request latency (in nanoseconds) using atomic operations
    pub fn record_live_latency_ns(&self, latency_ns: u64) {
        // Record to histogram for percentile calculation
        self.latency_histogram.record(latency_ns);

        // Add to sum
        self.live_latency_sum_ns.fetch_add(latency_ns, Ordering::Relaxed);
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
        let min_us = if min_ns == u64::MAX { 0.0 } else { min_ns as f64 / 1000.0 };
        let max_us = max_ns as f64 / 1000.0;

        (count, avg_us, min_us, max_us)
    }

    /// Log live latency stats with percentiles
    pub fn log_live_latency(&self) {
        let (count, avg_us, min_us, max_us, p50_us, p95_us, p99_us) =
            self.latency_histogram.get_percentiles_and_reset();

        // Also reset the simple counters to keep them in sync
        self.live_latency_sum_ns.swap(0, Ordering::Relaxed);
        self.live_latency_count.swap(0, Ordering::Relaxed);
        self.live_latency_min_ns.swap(u64::MAX, Ordering::Relaxed);
        self.live_latency_max_ns.swap(0, Ordering::Relaxed);

        if count > 0 {
            info!(
                "Live latency: {} reqs | avg: {:.1}µs | p50: {:.1}µs | p95: {:.1}µs | p99: {:.1}µs | min: {:.1}µs | max: {:.1}µs",
                count, avg_us, p50_us, p95_us, p99_us, min_us, max_us
            );
        }
    }
}