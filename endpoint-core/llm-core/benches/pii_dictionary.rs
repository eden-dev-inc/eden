//! Criterion benchmark for the custom PII dictionary matcher.
//!
//! Measures the cost the dictionary adds to the per-request PII path so we can
//! confirm it stays cheap as the dictionary grows:
//!   * `build`           — compiling N terms into the Aho-Corasick automaton
//!   * `scan_text`       — matching a realistic prompt (automaton already built)
//!   * `build_plus_scan` — full per-request cost if the matcher is NOT cached
//!   * `redact_text`     — masking matched terms in a prompt
//!
//! Run with: `cargo bench -p llm-core --bench pii_dictionary`

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use llm_core::{CustomPiiTerm, LlmPiiScanner, PolicyAction};

/// A synthetic dictionary of `n` terms with a mix of actions and a shared
/// prefix (a realistic-to-adversarial case for the trie).
fn dictionary(n: usize) -> Vec<CustomPiiTerm> {
    (0..n)
        .map(|i| {
            let action = match i % 3 {
                0 => PolicyAction::Redact,
                1 => PolicyAction::Block,
                _ => PolicyAction::AuditOnly,
            };
            CustomPiiTerm {
                term: format!("codename-{i:05}"),
                action,
                label: Some("project".to_string()),
            }
        })
        .collect()
}

/// A realistic ~`words`-word prompt that embeds a few dictionary terms and
/// built-in PII (email, phone).
fn prompt(words: usize) -> String {
    let mut text = String::with_capacity(words * 6);
    for i in 0..words {
        match i {
            10 => text.push_str("contact alice@example.com "),
            25 => text.push_str("re codename-00007 "),
            40 => text.push_str("call 555-123-4567 "),
            60 => text.push_str("see codename-00042 "),
            _ => text.push_str("lorem ipsum dolor "),
        }
    }
    text
}

fn bench_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("pii_dictionary/build");
    for &n in &[10usize, 100, 1000] {
        let terms = dictionary(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &terms, |b, terms| {
            b.iter(|| black_box(LlmPiiScanner::with_custom_terms(black_box(terms.clone()))));
        });
    }
    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let text = prompt(400);
    let mut group = c.benchmark_group("pii_dictionary/scan_text");
    group.throughput(Throughput::Bytes(text.len() as u64));
    // n = 0 is the built-in-only baseline; the rest show the dictionary's marginal cost.
    for &n in &[0usize, 10, 100, 1000] {
        let scanner = LlmPiiScanner::with_custom_terms(dictionary(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &scanner, |b, scanner| {
            b.iter(|| black_box(scanner.scan_text(black_box(&text))));
        });
    }
    group.finish();
}

fn bench_build_plus_scan(c: &mut Criterion) {
    // Full per-request cost when the matcher is rebuilt every request (current
    // behavior). If this is ever too high, cache the matcher per key.
    let text = prompt(400);
    let mut group = c.benchmark_group("pii_dictionary/build_plus_scan");
    for &n in &[10usize, 100, 1000] {
        let terms = dictionary(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &terms, |b, terms| {
            b.iter(|| {
                let scanner = LlmPiiScanner::with_custom_terms(terms.clone());
                black_box(scanner.scan_text(black_box(&text)))
            });
        });
    }
    group.finish();
}

fn bench_redact(c: &mut Criterion) {
    let text = prompt(400);
    let mut group = c.benchmark_group("pii_dictionary/redact_text");
    for &n in &[10usize, 100, 1000] {
        let scanner = LlmPiiScanner::with_custom_terms(dictionary(n));
        let scan = scanner.scan_text(&text);
        group.bench_with_input(BenchmarkId::from_parameter(n), &(scanner, scan), |b, (scanner, scan)| {
            b.iter(|| black_box(scanner.redact_text(black_box(&text), black_box(scan))));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_build, bench_scan, bench_build_plus_scan, bench_redact);
criterion_main!(benches);
