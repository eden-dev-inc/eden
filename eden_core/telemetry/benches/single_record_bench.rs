// Benchmark for single .record() call latency
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;

fn bench_single_histogram_record(c: &mut Criterion) {
    // Setup
    let provider = SdkMeterProvider::builder().build();
    let meter = provider.meter("bench");
    let histogram = meter.u64_histogram("test_duration").with_description("Test histogram").with_unit("microsec").build();

    let labels = vec![
        KeyValue::new("endpoint", "test"),
        KeyValue::new("method", "GET"),
        KeyValue::new("status", "200"),
    ];

    c.bench_function("single_record_with_3_labels", |b| {
        b.iter(|| {
            histogram.record(black_box(1234), black_box(&labels));
        })
    });
}

fn bench_single_record_no_labels(c: &mut Criterion) {
    let provider = SdkMeterProvider::builder().build();
    let meter = provider.meter("bench");
    let histogram = meter.u64_histogram("test_duration").build();

    c.bench_function("single_record_no_labels", |b| {
        b.iter(|| {
            histogram.record(black_box(1234), &[]);
        })
    });
}

fn bench_single_counter_add(c: &mut Criterion) {
    let provider = SdkMeterProvider::builder().build();
    let meter = provider.meter("bench");
    let counter = meter.u64_counter("test_count").build();

    let labels = vec![KeyValue::new("endpoint", "test"), KeyValue::new("method", "GET")];

    c.bench_function("single_counter_add_with_2_labels", |b| {
        b.iter(|| {
            counter.add(black_box(1), black_box(&labels));
        })
    });
}

fn bench_single_updown_counter(c: &mut Criterion) {
    let provider = SdkMeterProvider::builder().build();
    let meter = provider.meter("bench");
    let counter = meter.i64_up_down_counter("test_active").build();

    c.bench_function("single_updown_counter_add", |b| {
        b.iter(|| {
            counter.add(black_box(1), &[]);
        })
    });
}

criterion_group!(
    benches,
    bench_single_histogram_record,
    bench_single_record_no_labels,
    bench_single_counter_add,
    bench_single_updown_counter,
);
criterion_main!(benches);
