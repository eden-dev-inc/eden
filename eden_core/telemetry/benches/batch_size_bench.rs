use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use opentelemetry::metrics::{Counter, MeterProvider};
use opentelemetry_sdk::metrics::SdkMeterProvider;

fn bench_metrics(c: &mut Criterion) {
    let provider = SdkMeterProvider::builder().build(); // Simplified; use your setup
    let meter = provider.meter("bench");
    let counter: Counter<u64> = meter.u64_counter("test_counter").build();

    let mut group = c.benchmark_group("metrics_batching");
    for batch_size in [100, 500, 1000, 5000, 10000] {
        group.bench_function(format!("batch_{}", batch_size), |b| {
            b.iter_batched(
                || (0..batch_size).collect::<Vec<_>>(), // Simulate batch data
                |data| {
                    for _ in data {
                        counter.add(1, &[]);
                    }
                },
                BatchSize::SmallInput, // Or LargeInput for bigger tests
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_metrics);
criterion_main!(benches);
