//! Tool-command validator (safety) metrics using fast-telemetry.
//!
//! Records the safety classification of every command an agent/tool attempts to
//! run against an endpoint, across all endpoint kinds. Emitted with dynamic
//! labels (`endpoint_uuid`, `endpoint_kind`, and `safety`/`reason`) so safety can
//! be filtered by endpoint or by kind. Exported via both OTLP (Datadog) and the
//! native ClickHouse metric path, like every other Eden metric domain.
//!
//! # Potential fast-telemetry upgrades (deferred)
//!
//! These 0.5.x capabilities are available but intentionally not adopted yet —
//! each should only be taken on if it provides a concrete functional win:
//!
//! - **In-process live values via `visit_metrics`.** The `#[derive(ExportMetrics)]`
//!   macro generates `visit_metrics(&self, &mut impl MetricVisitor)`, a typed
//!   in-process traversal of these counters/histogram. A `MetricVisitor` in
//!   `eden_service` could surface this domain (and others) as JSON for the embedded
//!   single-node dashboard to render real values with **no ClickHouse dependency**.
//!   Caveat: it only reflects the local node, so it complements — not replaces —
//!   the fleet-wide ClickHouse analytics path. (Already used in the validator
//!   boundary unit tests to assert recorded counters.)
//! - **Cardinality-overflow visibility via `MetricVisitor::dynamic_overflow`.**
//!   These series are keyed on high-cardinality `endpoint_uuid`; the overflow
//!   callback reports when label series are dropped at the cap. Worth surfacing on
//!   an ops/debug view if/when caps are actually hit.
//! - **`LabelEnum` + `LabeledCounter<Safety>`.** The four `*_total` counters could
//!   collapse into one `LabeledCounter` keyed by a `#[derive(LabelEnum)]` safety
//!   enum. Purely a cleanup — no behavior change — so low priority.

use fast_telemetry::{DynamicCounter, DynamicDistribution, ExportMetrics};

/// Default shard count for thread-sharded counters.
const SHARD_COUNT: usize = 16;

/// Validator (tool-safety) metrics with dynamic per-endpoint labels.
#[derive(ExportMetrics)]
#[metric_prefix = "eden.validator"]
#[otlp]
#[clickhouse]
pub struct ValidatorMetrics {
    /// Commands classified as safe (read-only).
    #[help = "Tool commands classified as safe"]
    safe_total: DynamicCounter,

    /// Commands classified as moderate (mutating but generally safe).
    #[help = "Tool commands classified as moderate"]
    moderate_total: DynamicCounter,

    /// Commands classified as dangerous (destructive/administrative).
    #[help = "Tool commands classified as dangerous"]
    dangerous_total: DynamicCounter,

    /// Commands hard-blocked by the validator (forbidden operator/stage).
    #[help = "Tool commands blocked by the validator"]
    blocked_total: DynamicCounter,

    /// Distribution of validation durations (microseconds).
    #[help = "Tool command validation duration in microseconds"]
    duration_micros: DynamicDistribution,
}

impl ValidatorMetrics {
    pub fn new() -> Self {
        Self {
            safe_total: DynamicCounter::new(SHARD_COUNT),
            moderate_total: DynamicCounter::new(SHARD_COUNT),
            dangerous_total: DynamicCounter::new(SHARD_COUNT),
            blocked_total: DynamicCounter::new(SHARD_COUNT),
            duration_micros: DynamicDistribution::new(SHARD_COUNT),
        }
    }

    /// Record a `Safe` classification.
    #[inline]
    pub fn record_safe(&self, labels: &[(&str, &str)]) {
        self.safe_total.inc(labels);
    }

    /// Record a `Moderate` classification.
    #[inline]
    pub fn record_moderate(&self, labels: &[(&str, &str)]) {
        self.moderate_total.inc(labels);
    }

    /// Record a `Dangerous` classification.
    #[inline]
    pub fn record_dangerous(&self, labels: &[(&str, &str)]) {
        self.dangerous_total.inc(labels);
    }

    /// Record a hard block (validation rejected the command).
    #[inline]
    pub fn record_blocked(&self, labels: &[(&str, &str)]) {
        self.blocked_total.inc(labels);
    }

    /// Record validation latency in microseconds.
    #[inline]
    pub fn record_duration_micros(&self, micros: u64, labels: &[(&str, &str)]) {
        self.duration_micros.record(labels, micros);
    }

    /// Total cardinality across all dynamic validator series.
    pub fn cardinality(&self) -> usize {
        self.safe_total.cardinality()
            + self.moderate_total.cardinality()
            + self.dangerous_total.cardinality()
            + self.blocked_total.cardinality()
            + self.duration_micros.cardinality()
    }

    /// Evict series inactive for `max_staleness` export cycles. Returns the count evicted.
    pub fn evict_stale(&self, max_staleness: u32) -> usize {
        let mut evicted = 0;
        evicted += self.safe_total.evict_stale(max_staleness);
        evicted += self.moderate_total.evict_stale(max_staleness);
        evicted += self.dangerous_total.evict_stale(max_staleness);
        evicted += self.blocked_total.evict_stale(max_staleness);
        evicted += self.duration_micros.evict_stale(max_staleness);
        evicted
    }
}

impl Default for ValidatorMetrics {
    fn default() -> Self {
        Self::new()
    }
}
