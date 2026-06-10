use criterion::{Criterion, black_box, criterion_group, criterion_main};
use opentelemetry::{KeyValue, global, metrics::Histogram};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use opentelemetry_stdout::MetricExporterBuilder;
use rand::Rng;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

/// Simple async-free setup of an OTel histogram.
fn setup_otel() -> Histogram<f64> {
    let exporter = MetricExporterBuilder::default().build();
    let provider = SdkMeterProvider::builder()
        .with_resource(Resource::builder().with_service_name("eden.bench").build())
        .with_periodic_exporter(exporter)
        .build();
    global::set_meter_provider(provider);
    let meter = global::meter("eden.benchmark");
    meter.f64_histogram("bench_histogram").build()
}

/// Batched collector: thread-safe local buffer + manual flush
#[derive(Clone)]
struct LocalBatcher {
    buffer: Arc<Mutex<HashMap<&'static str, f64>>>,
    histogram: Histogram<f64>,
}

impl LocalBatcher {
    fn new(histogram: Histogram<f64>) -> Self {
        Self { buffer: Arc::new(Mutex::new(HashMap::new())), histogram }
    }

    fn record_local(&self, key: &'static str, value: f64) {
        let mut buf = self.buffer.lock().expect("Poisoned mutex");
        *buf.entry(key).or_insert(0.0) += value;
    }

    fn flush(&self) {
        let mut buf = self.buffer.lock().expect("Poisoned mutex");
        for (k, v) in buf.drain() {
            self.histogram.record(v, &[KeyValue::new("metric", k)]);
        }
    }
}

fn bench_direct_parallel(c: &mut Criterion) {
    let histogram = Arc::new(setup_otel());
    c.bench_function("otel_direct_parallel", |b| {
        b.iter(|| {
            let hist = histogram.clone();
            (0..1_000_000).into_par_iter().for_each(|_| {
                let val = rand::rng().random_range(0.0..100.0);
                hist.record(val, &[KeyValue::new("path", "/direct")]);
            });
            black_box(Instant::now());
        });
    });
}

fn bench_batched_parallel(c: &mut Criterion) {
    let histogram = setup_otel();
    let batcher = Arc::new(LocalBatcher::new(histogram));
    c.bench_function("otel_batched_parallel", |b| {
        b.iter(|| {
            let bch = batcher.clone();
            (0..1_000_000).into_par_iter().for_each(|_| {
                let val = rand::rng().random_range(0.0..100.0);
                bch.record_local("batched", val);
            });
            // simulate flush once per interval
            bch.flush();
            black_box(Instant::now());
        });
    });
}
fn configure_criterion() -> Criterion {
    Criterion::default().sample_size(10).measurement_time(std::time::Duration::from_secs(1))
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = bench_direct_parallel, bench_batched_parallel
}
criterion_main!(benches);
