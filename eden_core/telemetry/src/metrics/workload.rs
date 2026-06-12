//! Workload metrics for AMR profiling.
//!
//! These metrics track Redis workload characteristics for Azure Managed Redis
//! SKU recommendations.

use fast_telemetry::{DynamicGauge, ExportMetrics};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// AMR Workload Profile classification based on ops/sec to memory ratio
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum AmrProfile {
    /// High memory, low ops/sec (ratio < 1)
    Memory,
    /// Balanced workload (ratio 1-50)
    Balanced,
    /// High TPS, light memory (ratio > 50)
    Compute,
}

impl AmrProfile {
    /// Classify workload based on ops_per_sec / used_memory_mb ratio
    pub fn from_ratio(ratio: f64) -> Self {
        if ratio < 1.0 {
            AmrProfile::Memory
        } else if ratio <= 50.0 {
            AmrProfile::Balanced
        } else {
            AmrProfile::Compute
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            AmrProfile::Memory => "memory",
            AmrProfile::Balanced => "balanced",
            AmrProfile::Compute => "compute",
        }
    }

    /// Get recommended Azure SKU tier hint
    pub fn recommended_sku_hint(&self) -> &'static str {
        match self {
            AmrProfile::Memory => "Memory-optimized (M-series)",
            AmrProfile::Balanced => "General Purpose (P-series)",
            AmrProfile::Compute => "Compute-optimized (C-series)",
        }
    }
}

/// Default shard count for thread-sharded gauges.
const SHARD_COUNT: usize = 16;

/// Metrics for AMR workload profiling.
///
/// All metrics are DynamicGauges since they represent point-in-time snapshots
/// of the workload state with endpoint_uuid labels for per-endpoint tracking.
#[derive(ExportMetrics)]
#[metric_prefix = "workload"]
#[otlp]
#[clickhouse]
pub struct WorkloadMetrics {
    /// Average operations per second (with endpoint_uuid label)
    #[help = "Average operations per second"]
    avg_ops_per_sec: DynamicGauge,

    /// Used memory in megabytes (with endpoint_uuid label)
    #[help = "Used memory in megabytes"]
    used_memory_mb: DynamicGauge,

    /// Database size in gigabytes (with endpoint_uuid label)
    #[help = "Database size in gigabytes"]
    database_size_gb: DynamicGauge,

    /// Workload ratio: ops_per_sec / used_memory_mb (with endpoint_uuid label)
    #[help = "Workload ratio (ops_per_sec / used_memory_mb)"]
    workload_ratio: DynamicGauge,

    /// AMR profile classification (0=Memory, 1=Balanced, 2=Compute) (with endpoint_uuid label)
    #[help = "AMR profile classification (0=Memory, 1=Balanced, 2=Compute)"]
    amr_profile: DynamicGauge,

    /// Total keys in database (with endpoint_uuid label)
    #[help = "Total keys in database"]
    total_keys: DynamicGauge,

    /// Keys with TTL set (with endpoint_uuid label)
    #[help = "Keys with TTL set"]
    keys_with_ttl: DynamicGauge,

    /// Instantaneous ops/sec from Redis (with endpoint_uuid label)
    #[help = "Instantaneous ops/sec from Redis"]
    instantaneous_ops_per_sec: DynamicGauge,

    /// Total commands processed (cumulative) (with endpoint_uuid label)
    #[help = "Total commands processed (cumulative)"]
    total_commands_processed: DynamicGauge,

    /// Redis application processing (CPU seconds) (with endpoint_uuid label)
    #[help = "CPU time spent in user space (seconds)"]
    used_cpu_user: DynamicGauge,

    /// Number of currently connected clients (with endpoint_uuid label)
    #[help = "Number of currently connected clients"]
    connected_clients: DynamicGauge,
}

impl WorkloadMetrics {
    /// Create new WorkloadMetrics
    pub fn new() -> Self {
        Self {
            avg_ops_per_sec: DynamicGauge::new(SHARD_COUNT),
            used_memory_mb: DynamicGauge::new(SHARD_COUNT),
            database_size_gb: DynamicGauge::new(SHARD_COUNT),
            workload_ratio: DynamicGauge::new(SHARD_COUNT),
            amr_profile: DynamicGauge::new(SHARD_COUNT),
            total_keys: DynamicGauge::new(SHARD_COUNT),
            keys_with_ttl: DynamicGauge::new(SHARD_COUNT),
            instantaneous_ops_per_sec: DynamicGauge::new(SHARD_COUNT),
            total_commands_processed: DynamicGauge::new(SHARD_COUNT),
            used_cpu_user: DynamicGauge::new(SHARD_COUNT),
            connected_clients: DynamicGauge::new(SHARD_COUNT),
        }
    }

    /// Record a workload snapshot with all relevant metrics
    #[inline]
    pub fn record_snapshot(&self, snapshot: &WorkloadSnapshot, labels: &[(&str, &str)]) {
        log::debug!(
            "[METRIC_RECORDED] workload snapshot | ops/sec={}, memory_mb={}, ratio={:.2}, profile={:?}",
            snapshot.avg_ops_per_sec,
            snapshot.used_memory_mb,
            snapshot.workload_ratio,
            snapshot.amr_profile
        );

        self.avg_ops_per_sec.set(labels, snapshot.avg_ops_per_sec);
        self.used_memory_mb.set(labels, snapshot.used_memory_mb);
        self.database_size_gb.set(labels, snapshot.database_size_gb);
        self.workload_ratio.set(labels, snapshot.workload_ratio);
        self.amr_profile.set(labels, snapshot.amr_profile as i64 as f64);
        self.total_keys.set(labels, snapshot.total_keys as f64);
        self.keys_with_ttl.set(labels, snapshot.keys_with_ttl as f64);
        self.instantaneous_ops_per_sec.set(labels, snapshot.instantaneous_ops_per_sec as f64);
        self.total_commands_processed.set(labels, snapshot.total_commands_processed as f64);
        self.used_cpu_user.set(labels, snapshot.used_cpu_user);
        self.connected_clients.set(labels, snapshot.connected_clients as f64);
    }

    /// Record individual metrics (for incremental updates)
    #[inline]
    pub fn record_ops_per_sec(&self, ops_per_sec: f64, labels: &[(&str, &str)]) {
        self.avg_ops_per_sec.set(labels, ops_per_sec);
    }

    /// Record memory usage
    #[inline]
    pub fn record_memory(&self, memory_mb: f64, labels: &[(&str, &str)]) {
        self.used_memory_mb.set(labels, memory_mb);
    }

    /// Record workload ratio and update AMR profile
    #[inline]
    pub fn record_ratio(&self, ratio: f64, labels: &[(&str, &str)]) {
        self.workload_ratio.set(labels, ratio);
        let profile = AmrProfile::from_ratio(ratio);
        self.amr_profile.set(labels, profile as i64 as f64);
    }

    /// Get total series cardinality across all dynamic workload metrics.
    pub fn cardinality(&self) -> usize {
        self.avg_ops_per_sec.cardinality()
            + self.used_memory_mb.cardinality()
            + self.database_size_gb.cardinality()
            + self.workload_ratio.cardinality()
            + self.amr_profile.cardinality()
            + self.total_keys.cardinality()
            + self.keys_with_ttl.cardinality()
            + self.instantaneous_ops_per_sec.cardinality()
            + self.total_commands_processed.cardinality()
            + self.used_cpu_user.cardinality()
            + self.connected_clients.cardinality()
    }

    /// Evict stale series from all workload metrics.
    ///
    /// Series that haven't been accessed for `max_staleness` cycles are removed.
    /// Returns total number of series evicted.
    pub fn evict_stale(&self, max_staleness: u32) -> usize {
        let mut evicted = 0;
        evicted += self.avg_ops_per_sec.evict_stale(max_staleness);
        evicted += self.used_memory_mb.evict_stale(max_staleness);
        evicted += self.database_size_gb.evict_stale(max_staleness);
        evicted += self.workload_ratio.evict_stale(max_staleness);
        evicted += self.amr_profile.evict_stale(max_staleness);
        evicted += self.total_keys.evict_stale(max_staleness);
        evicted += self.keys_with_ttl.evict_stale(max_staleness);
        evicted += self.instantaneous_ops_per_sec.evict_stale(max_staleness);
        evicted += self.total_commands_processed.evict_stale(max_staleness);
        evicted += self.used_cpu_user.evict_stale(max_staleness);
        evicted += self.connected_clients.evict_stale(max_staleness);
        evicted
    }
}

impl Default for WorkloadMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A snapshot of workload metrics at a point in time
#[derive(Debug, Clone)]
pub struct WorkloadSnapshot {
    /// Average operations per second
    pub avg_ops_per_sec: f64,
    /// Used memory in megabytes
    pub used_memory_mb: f64,
    /// Database size in gigabytes
    pub database_size_gb: f64,
    /// Workload ratio: ops_per_sec / used_memory_mb
    pub workload_ratio: f64,
    /// AMR profile classification
    pub amr_profile: AmrProfile,
    /// Total keys in database
    pub total_keys: u64,
    /// Keys with TTL set
    pub keys_with_ttl: u64,
    /// Instantaneous ops/sec from Redis
    pub instantaneous_ops_per_sec: u64,
    /// Total commands processed
    pub total_commands_processed: u64,
    /// Endpoint ID this snapshot is for
    pub endpoint_id: String,
    /// Redis application processing
    pub used_cpu_user: f64,
    /// Number of currently connected clients
    pub connected_clients: u64,
}

impl WorkloadSnapshot {
    /// Create a new workload snapshot from raw Redis INFO values
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint_id: String,
        ops_per_sec: f64,
        used_memory_bytes: u64,
        total_keys: u64,
        keys_with_ttl: u64,
        instantaneous_ops_per_sec: u64,
        total_commands_processed: u64,
        used_cpu_user: f64,
        connected_clients: u64,
    ) -> Self {
        let used_memory_mb = used_memory_bytes as f64 / (1024.0 * 1024.0);
        let database_size_gb = used_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

        // Calculate workload ratio, avoiding division by zero
        let workload_ratio = if used_memory_mb > 0.0 { ops_per_sec / used_memory_mb } else { 0.0 };

        let amr_profile = AmrProfile::from_ratio(workload_ratio);

        Self {
            avg_ops_per_sec: ops_per_sec,
            used_memory_mb,
            database_size_gb,
            workload_ratio,
            amr_profile,
            total_keys,
            keys_with_ttl,
            instantaneous_ops_per_sec,
            total_commands_processed,
            endpoint_id,
            used_cpu_user,
            connected_clients,
        }
    }

    /// Get AMR deployment recommendation as a human-readable string
    pub fn get_recommendation(&self) -> String {
        format!(
            "Based on workload ratio {:.2} (ops/sec={:.0}, memory={:.1}MB): \
             Recommended AMR profile: {} - {}",
            self.workload_ratio,
            self.avg_ops_per_sec,
            self.used_memory_mb,
            self.amr_profile.as_str(),
            self.amr_profile.recommended_sku_hint()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_amr_profile_classification() {
        // Memory profile: ratio < 1
        assert_eq!(AmrProfile::from_ratio(0.0), AmrProfile::Memory);
        assert_eq!(AmrProfile::from_ratio(0.5), AmrProfile::Memory);
        assert_eq!(AmrProfile::from_ratio(0.99), AmrProfile::Memory);

        // Balanced profile: ratio 1-50
        assert_eq!(AmrProfile::from_ratio(1.0), AmrProfile::Balanced);
        assert_eq!(AmrProfile::from_ratio(25.0), AmrProfile::Balanced);
        assert_eq!(AmrProfile::from_ratio(50.0), AmrProfile::Balanced);

        // Compute profile: ratio > 50
        assert_eq!(AmrProfile::from_ratio(50.1), AmrProfile::Compute);
        assert_eq!(AmrProfile::from_ratio(100.0), AmrProfile::Compute);
        assert_eq!(AmrProfile::from_ratio(1000.0), AmrProfile::Compute);
    }

    #[test]
    fn test_workload_snapshot_creation() {
        // 1000 ops/sec, 100MB memory -> ratio = 10 -> Balanced
        let snapshot = WorkloadSnapshot::new(
            "test_endpoint".to_string(),
            1000.0,            // ops_per_sec
            100 * 1024 * 1024, // 100 MB in bytes
            50000,             // total_keys
            25000,             // keys_with_ttl
            1000,              // instantaneous_ops_per_sec
            1_000_000,         // total_commands_processed
            12.5,              // used_cpu_user
            42,                // connected_clients
        );

        assert!((snapshot.used_memory_mb - 100.0).abs() < 0.01);
        assert!((snapshot.workload_ratio - 10.0).abs() < 0.01);
        assert_eq!(snapshot.amr_profile, AmrProfile::Balanced);
        assert!((snapshot.used_cpu_user - 12.5).abs() < 0.01);
        assert_eq!(snapshot.connected_clients, 42);
    }

    #[test]
    fn test_memory_heavy_workload() {
        // 50 ops/sec, 1GB memory -> ratio = 0.05 -> Memory
        let snapshot = WorkloadSnapshot::new(
            "memory_heavy".to_string(),
            50.0,
            1024 * 1024 * 1024, // 1 GB
            1_000_000,
            500_000,
            50,
            10_000,
            5.0,
            10,
        );

        assert!(snapshot.workload_ratio < 1.0);
        assert_eq!(snapshot.amr_profile, AmrProfile::Memory);
    }

    #[test]
    fn test_compute_heavy_workload() {
        // 100000 ops/sec, 10MB memory -> ratio = 10000 -> Compute
        let snapshot = WorkloadSnapshot::new(
            "compute_heavy".to_string(),
            100_000.0,
            10 * 1024 * 1024, // 10 MB
            1000,
            500,
            100_000,
            50_000_000,
            150.0,
            500,
        );

        assert!(snapshot.workload_ratio > 50.0);
        assert_eq!(snapshot.amr_profile, AmrProfile::Compute);
    }
}
