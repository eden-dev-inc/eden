//! Metadata metrics for slowlog, latency, keyspace, and policy tracking.
//!
//! These metrics provide visibility into Redis metadata collection operations.

use fast_telemetry::{DynamicCounter, DynamicGauge, ExportMetrics};

/// Default shard count for thread-sharded counters.
const SHARD_COUNT: usize = 16;

/// Metrics for metadata collection (slowlog, latency, keyspace, policy).
///
/// Labels: endpoint_uuid, pattern, event_name, command
#[derive(ExportMetrics)]
#[metric_prefix = "redis"]
#[otlp]
#[clickhouse]
pub struct MetadataMetrics {
    // === Slowlog Metrics ===
    /// Gauge for slowlog entries collected per sync (with endpoint_uuid label)
    #[help = "Entries collected per sync"]
    slowlog_entries_collected: DynamicGauge,

    /// Counter for anti-patterns detected in slowlog (with endpoint_uuid, pattern labels)
    #[help = "Anti-patterns detected in slowlog"]
    slowlog_antipattern: DynamicCounter,

    // === Latency Metrics ===
    /// Latest latency per event type (with endpoint_uuid, event_name labels)
    #[help = "Latest latency per event type (ms)"]
    latency_event: DynamicGauge,

    // === Keyspace Metrics ===
    /// Percentage of sampled keys without TTL (with endpoint_uuid label)
    #[help = "Percentage of sampled keys without TTL"]
    keyspace_no_ttl_percentage: DynamicGauge,

    /// Number of keys over size threshold (with endpoint_uuid label)
    #[help = "Number of keys over threshold"]
    keyspace_big_keys_count: DynamicGauge,

    // === Policy Metrics ===
    /// Commands blocked by policy (with endpoint_uuid, command labels)
    #[help = "Commands blocked by policy"]
    policy_blocked: DynamicCounter,

    /// Commands warned by policy (with endpoint_uuid, command labels)
    #[help = "Commands warned by policy"]
    policy_warned: DynamicCounter,
}

impl MetadataMetrics {
    pub fn new() -> Self {
        Self {
            // Slowlog metrics
            slowlog_entries_collected: DynamicGauge::new(SHARD_COUNT),
            slowlog_antipattern: DynamicCounter::new(SHARD_COUNT),

            // Latency metrics (with endpoint_uuid, event_name labels)
            latency_event: DynamicGauge::new(SHARD_COUNT),

            // Keyspace metrics (with endpoint_uuid label)
            keyspace_no_ttl_percentage: DynamicGauge::new(SHARD_COUNT),
            keyspace_big_keys_count: DynamicGauge::new(SHARD_COUNT),

            // Policy metrics
            policy_blocked: DynamicCounter::new(SHARD_COUNT),
            policy_warned: DynamicCounter::new(SHARD_COUNT),
        }
    }

    // === Slowlog Methods ===

    /// Record the number of slowlog entries collected
    #[inline]
    pub fn record_entries_collected(&self, count: i64, labels: &[(&str, &str)]) {
        self.slowlog_entries_collected.set(labels, count as f64);
    }

    /// Record anti-pattern detection
    #[inline]
    pub fn record_antipattern(&self, count: u64, labels: &[(&str, &str)]) {
        self.slowlog_antipattern.add(labels, count as isize);
    }

    // === Latency Methods ===

    /// Record latency event (from LATENCY LATEST)
    #[inline]
    pub fn record_latency_event(&self, latency_ms: f64, labels: &[(&str, &str)]) {
        self.latency_event.set(labels, latency_ms);
    }

    // === Keyspace Methods ===

    /// Record percentage of keys without TTL
    #[inline]
    pub fn record_no_ttl_percentage(&self, percentage: f64, labels: &[(&str, &str)]) {
        self.keyspace_no_ttl_percentage.set(labels, percentage);
    }

    /// Record count of big keys
    #[inline]
    pub fn record_big_keys_count(&self, count: u64, labels: &[(&str, &str)]) {
        self.keyspace_big_keys_count.set(labels, count as f64);
    }

    // === Policy Methods ===

    /// Record command blocked by policy
    #[inline]
    pub fn record_policy_blocked(&self, count: u64, labels: &[(&str, &str)]) {
        self.policy_blocked.add(labels, count as isize);
    }

    /// Record command warned by policy
    #[inline]
    pub fn record_policy_warned(&self, count: u64, labels: &[(&str, &str)]) {
        self.policy_warned.add(labels, count as isize);
    }

    /// Get total series cardinality across all dynamic metadata metrics.
    pub fn cardinality(&self) -> usize {
        self.slowlog_entries_collected.cardinality()
            + self.latency_event.cardinality()
            + self.keyspace_no_ttl_percentage.cardinality()
            + self.keyspace_big_keys_count.cardinality()
            + self.slowlog_antipattern.cardinality()
            + self.policy_blocked.cardinality()
            + self.policy_warned.cardinality()
    }

    /// Evict stale series from all metadata metrics.
    ///
    /// Series that haven't been accessed for `max_staleness` cycles are removed.
    /// Returns total number of series evicted.
    pub fn evict_stale(&self, max_staleness: u32) -> usize {
        let mut evicted = 0;
        // Gauges
        evicted += self.slowlog_entries_collected.evict_stale(max_staleness);
        evicted += self.latency_event.evict_stale(max_staleness);
        evicted += self.keyspace_no_ttl_percentage.evict_stale(max_staleness);
        evicted += self.keyspace_big_keys_count.evict_stale(max_staleness);
        // Counters
        evicted += self.slowlog_antipattern.evict_stale(max_staleness);
        evicted += self.policy_blocked.evict_stale(max_staleness);
        evicted += self.policy_warned.evict_stale(max_staleness);
        evicted
    }
}

impl Default for MetadataMetrics {
    fn default() -> Self {
        Self::new()
    }
}
