use std::{
    env,
    sync::{Mutex, OnceLock},
    time::Duration,
};

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, MeterProvider},
};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_stdout::MetricExporterBuilder;

use crate::config::Config;

pub static LB_METRICS: OnceLock<Mutex<LbMetrics>> = OnceLock::new();

pub struct LbMetrics {
    requests_received: Counter<u64>,
    request_latency: Histogram<u64>,
}

impl LbMetrics {
    pub fn new(config: &Config) -> Self {
        unsafe {
            env::set_var("OTEL_SERVICE_NAME", "load_balancer");
        }
        let provider = if config.otlp_collector().is_empty() {
            // Console exporter for local dev/debug
            let exporter = MetricExporterBuilder::default().build();
            SdkMeterProvider::builder().with_periodic_exporter(exporter).build()
        } else {
            // OTLP exporter for Datadog/Collector
            match MetricExporter::builder()
                .with_tonic()
                .with_endpoint(config.otlp_collector())
                .with_timeout(Duration::from_secs(3))
                .build()
            {
                Ok(exporter) => SdkMeterProvider::builder().with_periodic_exporter(exporter).build(),
                Err(_) => {
                    let exporter = MetricExporterBuilder::default().build();
                    SdkMeterProvider::builder().with_periodic_exporter(exporter).build()
                }
            }
        };

        // Get a meter from the provider
        let meter = provider.meter("request_client");
        let requests_received = meter
            .u64_counter("eden_load_balancer_requests_received")
            .with_description("Total number of requests")
            .with_unit("requests")
            .build();
        let request_latency = meter
            .u64_histogram("eden_load_balancer_request_latency")
            .with_description("Total latency of requests")
            .with_unit("microsec")
            .build();
        opentelemetry::global::set_meter_provider(provider);
        Self { requests_received, request_latency }
    }

    pub fn add_request(&self, labels: &[KeyValue]) {
        self.requests_received.add(1, labels);
    }

    pub fn add_latency(&self, latency: u64, labels: &[KeyValue]) {
        self.request_latency.record(latency, labels);
    }
}
