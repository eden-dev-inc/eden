// Prometheus Metrics Collection
//
// This module defines and manages all Prometheus metrics for monitoring
// the analytics demo. It tracks performance, business metrics, and
// infrastructure health to demonstrate migration impact.

use prometheus::{CounterVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntGauge, Opts, Registry};

/// AppMetrics contains all Prometheus metrics for the application
/// Organized by category: events, queries, database, cache, and business metrics
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
}

impl AppMetrics {
    /// Create a new metrics registry with all application metrics
    /// Each metric is registered with Prometheus for collection
    pub fn new() -> Self {
        let registry = Registry::new();

        // Event generation metrics
        let events_generated_total = IntCounter::new(
            "events_generated_total",
            "Total number of events generated"
        ).unwrap();

        // Create a CounterVec with labels instead of a simple Counter
        let events_by_type = CounterVec::new(
            Opts::new("events_by_type_total", "Total events by type"),
            &["event_type"] // Label names
        ).unwrap();

        let event_generation_duration = Histogram::with_opts(
            HistogramOpts::new(
                "event_generation_duration_seconds",
                "Time spent generating events"
            )
        ).unwrap();

        // Query execution metrics
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
            &["operation", "result"] // get/set/del, hit/miss/error
        ).unwrap();

        let db_operation_duration = HistogramVec::new(
            HistogramOpts::new("db_operation_duration_seconds", "Database operation latency"),
            &["query_type", "result"] // select/insert/update, success/error
        ).unwrap();

        // Database metrics
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

        // Redis metrics
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

        // Business metrics
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

        // Register all metrics with Prometheus
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
        }
    }

    /// Record a generated event with type label
    /// Increments both total counter and per-type counter
    pub fn record_event_generated(&self, event_type: &str) {
        self.events_generated_total.inc();
        self.events_by_type.with_label_values(&[event_type]).inc();
    }

    /// Record query execution with duration and cache hit status
    /// Tracks both performance and cache efficiency
    pub fn record_query_executed(&self, duration: f64, cache_hit: bool) {
        self.queries_executed_total.inc();
        self.query_duration.observe(duration);

        if cache_hit {
            self.cache_hits_total.inc();
        } else {
            self.cache_misses_total.inc();
        }
    }

    /// Record database query execution time
    /// Used to monitor database performance during migrations
    pub fn record_db_query(&self, duration: f64) {
        self.db_queries_total.inc();
        self.db_query_duration.observe(duration);
    }

    /// Record Redis operation execution time
    /// Monitors cache performance and latency
    pub fn record_redis_operation(&self, duration: f64) {
        self.redis_operations_total.inc();
        self.redis_operation_duration.observe(duration);
    }

    /// Update high-level business metrics
    /// Called periodically to maintain current operational state
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
}