use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use redis_core::validation::{empty_read_budget_for_validation, timed_duplex_read_for_validation};
use tokio::runtime::Runtime;

#[derive(Clone, Copy)]
enum LoadProfile {
    Consistent,
    Variable,
    Malicious,
}

impl LoadProfile {
    fn label(self) -> &'static str {
        match self {
            Self::Consistent => "consistent",
            Self::Variable => "variable",
            Self::Malicious => "malicious",
        }
    }

    fn poll_interval(self) -> std::time::Duration {
        match self {
            Self::Consistent => std::time::Duration::from_millis(5),
            Self::Variable => std::time::Duration::from_millis(20),
            Self::Malicious => std::time::Duration::from_millis(50),
        }
    }

    fn prefilled_bytes(self) -> usize {
        match self {
            Self::Consistent => 0,
            Self::Variable => 256,
            Self::Malicious => 4096,
        }
    }
}

fn bench_read_detection(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let mut group = c.benchmark_group("redis_read_detection");

    for profile in [LoadProfile::Consistent, LoadProfile::Variable, LoadProfile::Malicious] {
        group.throughput(Throughput::Elements(1));

        group.bench_function(BenchmarkId::new("idle_peer", profile.label()), |b| {
            b.iter(|| {
                runtime.block_on(async {
                    let (read, elapsed) = timed_duplex_read_for_validation(false, profile.poll_interval(), profile.prefilled_bytes())
                        .await
                        .expect("idle read succeeds");
                    black_box((read, elapsed.as_nanos(), empty_read_budget_for_validation()))
                })
            })
        });

        group.bench_function(BenchmarkId::new("half_closed_peer", profile.label()), |b| {
            b.iter(|| {
                runtime.block_on(async {
                    let (read, elapsed) = timed_duplex_read_for_validation(true, profile.poll_interval(), profile.prefilled_bytes())
                        .await
                        .expect("half-closed read succeeds");
                    black_box((read, elapsed.as_nanos(), empty_read_budget_for_validation()))
                })
            })
        });
    }

    group.finish();
}

criterion_group!(read_detection_bench, bench_read_detection);
criterion_main!(read_detection_bench);
