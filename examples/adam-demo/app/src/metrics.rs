use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
};
use std::sync::Arc;

/// Application metrics exposed via /metrics endpoint.
pub struct AppMetrics {
    pub registry: Registry,

    // Query metrics per database
    pub queries_total: CounterVec,
    pub query_errors: CounterVec,
    pub query_duration: HistogramVec,

    // Cross-database query metrics
    pub cross_db_queries_total: Counter,
    pub cross_db_query_duration: Histogram,

    // Active state
    #[allow(dead_code)]
    pub active_workers: Gauge,
    pub queries_per_second: GaugeVec,

    // Endpoint health
    pub endpoint_healthy: GaugeVec,
}

impl AppMetrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();
        let db_labels = &["database"];

        let queries_total = CounterVec::new(
            Opts::new("adam_queries_total", "Total queries executed per database"),
            db_labels,
        )
        .unwrap();

        let query_errors = CounterVec::new(
            Opts::new("adam_query_errors_total", "Total query errors per database"),
            db_labels,
        )
        .unwrap();

        let query_duration = HistogramVec::new(
            HistogramOpts::new("adam_query_duration_seconds", "Query latency per database")
                .buckets(vec![
                    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
                ]),
            db_labels,
        )
        .unwrap();

        let cross_db_queries_total = Counter::with_opts(Opts::new(
            "adam_cross_db_queries_total",
            "Total cross-database queries",
        ))
        .unwrap();

        let cross_db_query_duration = Histogram::with_opts(
            HistogramOpts::new(
                "adam_cross_db_query_duration_seconds",
                "Cross-database query latency",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        )
        .unwrap();

        let active_workers = Gauge::with_opts(Opts::new(
            "adam_active_workers",
            "Current active query workers",
        ))
        .unwrap();

        let queries_per_second = GaugeVec::new(
            Opts::new("adam_queries_per_second", "Current QPS per database"),
            db_labels,
        )
        .unwrap();

        let endpoint_healthy = GaugeVec::new(
            Opts::new(
                "adam_endpoint_healthy",
                "Endpoint health status (1=up, 0=down)",
            ),
            db_labels,
        )
        .unwrap();

        // Register all metrics
        registry.register(Box::new(queries_total.clone())).unwrap();
        registry.register(Box::new(query_errors.clone())).unwrap();
        registry.register(Box::new(query_duration.clone())).unwrap();
        registry
            .register(Box::new(cross_db_queries_total.clone()))
            .unwrap();
        registry
            .register(Box::new(cross_db_query_duration.clone()))
            .unwrap();
        registry.register(Box::new(active_workers.clone())).unwrap();
        registry
            .register(Box::new(queries_per_second.clone()))
            .unwrap();
        registry
            .register(Box::new(endpoint_healthy.clone()))
            .unwrap();

        Arc::new(Self {
            registry,
            queries_total,
            query_errors,
            query_duration,
            cross_db_queries_total,
            cross_db_query_duration,
            active_workers,
            queries_per_second,
            endpoint_healthy,
        })
    }
}
