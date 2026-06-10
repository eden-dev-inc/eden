use criterion::{Criterion, black_box, criterion_group, criterion_main};
use opentelemetry::metrics::{Histogram, MeterProvider};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use opentelemetry_stdout::MetricExporterBuilder;
use std::sync::Arc;

/// Setup OpenTelemetry provider + histogram
fn setup_meter() -> (SdkMeterProvider, Histogram<f64>) {
    let exporter = MetricExporterBuilder::default().build();
    let provider = SdkMeterProvider::builder()
        .with_resource(Resource::builder().with_service_name("eden.bench").build())
        .with_periodic_exporter(exporter)
        .build();

    let meter = provider.meter("benchmark_meter");
    let histogram = meter.f64_histogram("request_latency_ms").build();
    (provider, histogram)
}

/// Strategy A: direct flush every 1000 records
fn bench_direct_flush(c: &mut Criterion) {
    let (provider, histogram) = setup_meter();
    let histogram = Arc::new(histogram);
    let test_vec = vec![1.23_f64; 20_000];
    let attributes = vec![];

    c.bench_function("direct_flush_every_1000", |b| {
        b.iter(|| {
            for (i, &val) in test_vec.iter().enumerate() {
                histogram.record(black_box(val), &attributes);
                if i % 1000 == 0 {
                    let _ = provider.force_flush();
                }
            }
        });
    });
}

// /// Strategy B: accumulate in Vec, flush in batches
// fn bench_vector_batch_flush(c: &mut Criterion) {
//     let (provider, histogram) = setup_meter();
//     let histogram = Arc::new(histogram);
//     let attributes = vec![];

//     c.bench_function("batched_vector_flush_1000", |b| {
//         b.iter(|| {
//             let mut local_batch = Vec::with_capacity(1000);

//             // Simulate 20k records in total
//             for i in 0..20_000 {
//                 local_batch.push(black_box(1.23_f64));

//                 if local_batch.len() >= 1000 {
//                     // Flush the batch
//                     for v in local_batch.drain(..) {
//                         histogram.record(v, &attributes);
//                     }
//                     provider.force_flush();
//                 }
//             }

//             // Final remainder flush (if not divisible)
//             if !local_batch.is_empty() {
//                 for v in local_batch.drain(..) {
//                     histogram.record(v, &attributes);
//                 }
//                 provider.force_flush();
//             }
//         });
//     });
// }

criterion_group!(benches, bench_direct_flush);
criterion_main!(benches);
