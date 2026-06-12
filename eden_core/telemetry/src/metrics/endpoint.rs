//! Endpoint metrics using fast-telemetry for high-performance counting.
//!
//! Tracks per-endpoint request metrics with dynamic labels (endpoint_uuid, endpoint_type, org_uuid).

use crate::duration::DurationNanos;
use crate::labels::{LABEL_ORG_UUID, SYSTEM_ORG_UUID};
use fast_telemetry::DynamicLabelSet;
use fast_telemetry::{DynamicCounter, DynamicDistribution, DynamicGaugeI64, ExportMetrics};
use std::borrow::Cow;

/// Default shard count for thread-sharded counters.
const SHARD_COUNT: usize = 16;

fn labels_with_org_uuid<'a>(labels: &'a [(&'a str, &'a str)]) -> Cow<'a, [(&'a str, &'a str)]> {
    if labels.iter().any(|(key, value)| (*key == LABEL_ORG_UUID || *key == "organization_uuid") && !value.is_empty()) {
        Cow::Borrowed(labels)
    } else {
        let mut labels_with_org = Vec::with_capacity(labels.len() + 1);
        labels_with_org.extend_from_slice(labels);
        labels_with_org.push((LABEL_ORG_UUID, SYSTEM_ORG_UUID));
        Cow::Owned(labels_with_org)
    }
}

/// Endpoint metrics with descriptions for export.
///
/// Uses dynamic labels from TelemetryLabels (endpoint_uuid, endpoint_type, org_uuid, etc.)
/// for per-endpoint observability.
#[derive(ExportMetrics)]
#[metric_prefix = "eden.endpoint"]
#[otlp]
#[clickhouse]
pub struct EndpointMetrics {
    /// Number of active endpoint requests (gauge - current in-flight count)
    #[help = "Number of active endpoint requests"]
    active_requests: DynamicGaugeI64,

    /// Number of total endpoint requests
    #[help = "Total number of endpoint requests"]
    total_requests: DynamicCounter,

    /// Tracks the distribution of endpoint durations (in microseconds)
    #[help = "Distribution of endpoint request durations in microseconds"]
    endpoint_duration: DynamicDistribution,
}

impl EndpointMetrics {
    pub fn new() -> Self {
        Self {
            active_requests: DynamicGaugeI64::new(SHARD_COUNT),
            total_requests: DynamicCounter::new(SHARD_COUNT),
            endpoint_duration: DynamicDistribution::new(SHARD_COUNT),
        }
    }

    /// Start a new request.
    #[inline]
    pub fn start_endpoint_request(&self, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.active_requests.inc(labels);
        self.total_requests.inc(labels);
    }

    /// Finish the request with typed duration.
    #[inline]
    pub fn finish_endpoint_request(&self, duration: DurationNanos, labels: &[(&str, &str)]) {
        let labels = labels_with_org_uuid(labels);
        let labels = labels.as_ref();
        self.endpoint_duration.record(labels, duration.as_micros());
        self.active_requests.dec(labels);
    }

    /// Finish request with raw nanosecond duration.
    #[inline]
    pub fn finish_endpoint_request_nanos(&self, duration_nanos: u64, labels: &[(&str, &str)]) {
        self.finish_endpoint_request(DurationNanos::new(duration_nanos), labels);
    }

    /// Get current active request count (sum across all label sets).
    pub fn get_active_requests(&self) -> i64 {
        self.active_requests.sum_all()
    }

    /// Get total request count (sum across all label sets).
    pub fn get_total_requests(&self) -> u64 {
        self.total_requests.sum_all() as u64
    }

    /// Snapshot current cumulative endpoint request totals grouped by dynamic labels.
    pub fn snapshot_total_requests(&self) -> Vec<(DynamicLabelSet, isize)> {
        self.total_requests.snapshot()
    }

    /// Get total series cardinality across all dynamic endpoint metrics.
    pub fn cardinality(&self) -> usize {
        self.active_requests.cardinality() + self.total_requests.cardinality() + self.endpoint_duration.cardinality()
    }

    /// Evict stale series from all endpoint metrics.
    ///
    /// Series that haven't been accessed for `max_staleness` cycles are removed.
    /// Returns total number of series evicted.
    pub fn evict_stale(&self, max_staleness: u32) -> usize {
        let mut evicted = 0;
        evicted += self.active_requests.evict_stale(max_staleness);
        evicted += self.total_requests.evict_stale(max_staleness);
        evicted += self.endpoint_duration.evict_stale(max_staleness);
        evicted
    }
}

impl Default for EndpointMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_telemetry::clickhouse::ClickHouseMetricBatch;

    #[test]
    fn endpoint_metrics_add_system_org_when_missing() {
        let metrics = EndpointMetrics::new();
        metrics.start_endpoint_request(&[("endpoint_uuid", "endpoint-1")]);
        metrics.finish_endpoint_request_nanos(1_000, &[("endpoint_uuid", "endpoint-1")]);

        let mut batch = ClickHouseMetricBatch::new("eden-service");
        metrics.export_clickhouse(&mut batch, 123);

        assert!(batch.sums.iter().all(|row| row.Attributes.get(LABEL_ORG_UUID).is_some_and(|value| value == SYSTEM_ORG_UUID)));
        assert!(batch.exp_histograms.iter().all(|row| row.Attributes.get(LABEL_ORG_UUID).is_some_and(|value| value == SYSTEM_ORG_UUID)));
    }
}
