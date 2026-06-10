//! IAM (Identity and Access Management) metrics using fast-telemetry.

use fast_telemetry::{DynamicCounter, ExportMetrics};

/// Default shard count for thread-sharded counters.
const SHARD_COUNT: usize = 16;

/// Struct containing all the metrics for IAM information.
///
/// Labels: perms, resource_type, resource_id (optional)
#[derive(ExportMetrics)]
#[metric_prefix = "eden.iam"]
#[otlp]
#[clickhouse]
pub struct IamMetrics {
    /// Count of control-plane assignments grouped by exact permission string.
    #[help = "Total count of IAM assignments by exact control-plane permission string"]
    assignments: DynamicCounter,
}

impl IamMetrics {
    pub fn new() -> Self {
        Self { assignments: DynamicCounter::new(SHARD_COUNT) }
    }

    #[inline]
    pub fn add_assignments(&self, count: i64, labels: &[(&str, &str)]) {
        self.assignments.add(labels, count as isize);
    }

    #[inline]
    pub fn add_assignment(&self, labels: &[(&str, &str)]) {
        self.assignments.inc(labels);
    }

    #[inline]
    pub fn remove_assignment(&self, labels: &[(&str, &str)]) {
        self.assignments.add(labels, -1);
    }

    /// Get total series cardinality across all dynamic IAM metrics.
    pub fn cardinality(&self) -> usize {
        self.assignments.cardinality()
    }

    /// Evict stale series from all IAM metrics.
    ///
    /// Series that haven't been accessed for `max_staleness` cycles are removed.
    /// Returns total number of series evicted.
    pub fn evict_stale(&self, max_staleness: u32) -> usize {
        self.assignments.evict_stale(max_staleness)
    }
}

impl Default for IamMetrics {
    fn default() -> Self {
        Self::new()
    }
}
