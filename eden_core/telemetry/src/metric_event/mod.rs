mod metadata;
mod proxy;
mod snapshot;
mod workload;

use crate::{AllMetrics, TelemetryWrapper};
use format::rbac::ControlPerms;
use std::sync::Arc;

/// High-frequency source-read governor snapshot for Redis
#[derive(Debug, Clone, Copy)]
pub struct GovernorTelemetrySnapshot {
    pub cpu_pct: f64,
    pub mode: &'static str,
    pub read_batch_size: usize,
    pub total_probe_ups: u32,
    pub total_backoffs: u32,
    pub total_panics: u32,
}

/// Type of cache being tracked
#[derive(Debug, Clone, Copy)]
pub enum CacheKind {
    Redis,
    Local,
    Template,
}

impl CacheKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            CacheKind::Redis => "redis",
            CacheKind::Local => "local",
            CacheKind::Template => "template",
        }
    }
}

#[derive(Debug, Clone)]
pub enum MetricEvent<'a> {
    Login {
        org_uuid: &'a str,
    },

    LoginWith {
        user_id: &'a str,
        org_id: &'a str,
        org_uuid: &'a str,
    },

    CacheHit {
        /// Empty string when org uuid is unavailable
        org_uuid: &'a str,
        kind: CacheKind,
    },

    CacheMiss {
        /// Empty string when org uuid is unavailable
        org_uuid: &'a str,
        kind: CacheKind,
    },

    RoleGranted {
        org_uuid: &'a str,
        perms: ControlPerms,
        resource: Option<&'a str>,
        resource_id: Option<&'a str>,
    },

    RoleRevoked {
        org_uuid: &'a str,
        perms: ControlPerms,
        resource: Option<&'a str>,
        resource_id: Option<&'a str>,
    },

    RolesGrantedBatch {
        org_uuid: &'a str,
        perms: ControlPerms,
        count: i64,
    },

    ConnectionAcquired {
        org_uuid: &'a str,
        db_type: Option<&'a str>,
    },

    // === Snapshot Events ===
    /// Snapshot execution started
    SnapshotStarted {
        org_uuid: &'a str,
        snapshot_id: &'a str,
        source_endpoint_id: &'a str,
        target_count: usize,
    },

    /// Snapshot execution completed
    SnapshotCompleted {
        org_uuid: &'a str,
        snapshot_id: &'a str,
        duration_secs: f64,
        target_count: usize,
        batches_total: u64,
    },

    /// Snapshot execution failed
    SnapshotFailed {
        org_uuid: &'a str,
        snapshot_id: &'a str,
        duration_secs: f64,
        error_type: &'a str,
    },

    /// Per-target write result in a fan-out snapshot
    SnapshotTargetWrite {
        org_uuid: &'a str,
        snapshot_id: &'a str,
        target_endpoint_id: &'a str,
        success: bool,
        duration_secs: f64,
    },

    /// Bytes read from source during a snapshot
    SnapshotBytesRead {
        org_uuid: &'a str,
        snapshot_id: &'a str,
        bytes: u64,
    },

    /// Bytes written to a target during a snapshot
    SnapshotBytesWritten {
        org_uuid: &'a str,
        snapshot_id: &'a str,
        target_endpoint_id: &'a str,
        bytes: u64,
    },

    /// Snapshot scheduler poll cycle
    SnapshotSchedulerPoll {
        org_uuid: &'a str,
        snapshots_due: usize,
    },

    /// CDC WAL poll cycle (rows consumed from WAL)
    CdcWalPoll {
        org_uuid: &'a str,
        snapshot_id: &'a str,
        rows_consumed: u64,
    },

    /// CDC batch flushed to destination
    CdcFlush {
        org_uuid: &'a str,
        snapshot_id: &'a str,
        rows_written: u64,
        duration_secs: f64,
    },

    /// CDC backfill completed
    CdcBackfillCompleted {
        org_uuid: &'a str,
        snapshot_id: &'a str,
        rows_total: u64,
        duration_secs: f64,
    },

    /// CDC worker active count gauge
    CdcActiveWorkers {
        org_uuid: &'a str,
        count: usize,
    },

    /// Proxy request processed (one per batch of data received over the wire)
    ProxyRequest {
        org_uuid: &'a str,
        interlay_uuid: &'a str,
        endpoint_uuid: &'a str,
        command_type: Option<&'a str>,
        duration_us: u64,
        bytes_read: u64,
        bytes_written: u64,
        /// Number of Redis commands in this request batch
        command_count: u64,
    },

    /// Network I/O latency
    NetworkLatency {
        org_uuid: &'a str,
        endpoint_uuid: &'a str,
        endpoint_kind: &'a str,
        duration_us: u64,
    },

    /// Proxy error
    ProxyError {
        org_uuid: &'a str,
        interlay_uuid: &'a str,
        error_type: &'a str, // "parse_error", "timeout", "connection_error"
    },

    /// Proxy connection failure
    ProxyConnectionFailure {
        org_uuid: &'a str,
        interlay_uuid: &'a str,
        error_type: &'a str, // "tls_error", "bind_error", "accept_error"
    },

    // === Workload Profiling Events (AMR Migration) ===
    /// Complete workload snapshot for AMR profiling
    /// Used to calculate ops_per_sec / used_memory_mb ratio for AMR instance recommendation
    WorkloadSnapshot {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        avg_ops_per_sec: f64,
        used_memory_bytes: u64,
        total_keys: u64,
        keys_with_ttl: u64,
        instantaneous_ops_per_sec: u64,
        total_commands_processed: u64,
        /// CPU time spent in user space (Redis application processing)
        used_cpu_user: f64,
        /// Number of currently connected clients
        connected_clients: u64,
    },

    /// Update operations per second metric
    WorkloadOpsPerSec {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        ops_per_sec: f64,
    },

    /// Update memory usage metric
    WorkloadMemory {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        memory_mb: f64,
    },

    /// Update workload ratio metric (ops_per_sec / memory_mb)
    WorkloadRatio {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        ratio: f64,
    },

    // === Metadata Collection Events ===
    /// Slowlog entries collected per sync
    SlowlogEntriesCollected {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        entries: i64,
    },

    /// Anti-pattern detected in slowlog
    SlowlogAntipattern {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        pattern: &'a str,
        count: u64,
    },

    /// Latest latency per event type (from LATENCY LATEST)
    LatencyEvent {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        event_name: &'a str,
        latency_ms: f64,
    },

    /// Percentage of sampled keys without TTL
    KeyspaceNoTtlPercentage {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        percentage: f64,
    },

    /// Number of keys over size threshold
    KeyspaceBigKeysCount {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        count: u64,
    },

    /// Command blocked by policy
    PolicyBlocked {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        command: &'a str,
        count: u64,
    },

    /// Command warned by policy
    PolicyWarned {
        org_uuid: &'a str,
        endpoint_id: &'a str,
        command: &'a str,
        count: u64,
    },
}

impl<'a> MetricEvent<'a> {
    fn iam_labels(perms: &ControlPerms, resource: Option<&'a str>, resource_id: Option<&'a str>) -> Vec<(&'static str, String)> {
        let mut labels = vec![("perms", perms.to_perm_string())];
        if let Some(resource) = resource {
            labels.push(("resource_type", resource.to_string()));
        }
        if let Some(resource_id) = resource_id {
            labels.push(("resource_id", resource_id.to_string()));
        }
        labels
    }

    /// Record this metric event using the given metrics
    pub fn record(&self, metrics: &Arc<AllMetrics>) {
        match self {
            MetricEvent::Login { org_uuid } => {
                let labels: &[(&str, &str)] = &[("org_uuid", org_uuid)];
                metrics.eden().add_login(labels);
            }

            MetricEvent::LoginWith { user_id, org_id, org_uuid } => {
                let labels: &[(&str, &str)] = &[("user_id", user_id), ("org_id", org_id), ("org_uuid", org_uuid)];
                metrics.eden().add_login(labels);
            }

            MetricEvent::CacheHit { org_uuid, kind } => {
                let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("cache_type", kind.as_str())];
                match kind {
                    CacheKind::Redis => metrics.eden().add_redis_cache_hit(labels),
                    CacheKind::Local => metrics.eden().add_local_cache_hit(labels),
                    CacheKind::Template => metrics.eden().add_local_cache_hit(labels),
                }
            }

            MetricEvent::CacheMiss { org_uuid, kind } => {
                let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("cache_type", kind.as_str())];
                match kind {
                    CacheKind::Redis => metrics.eden().add_redis_cache_miss(labels),
                    CacheKind::Local => metrics.eden().add_local_cache_miss(labels),
                    CacheKind::Template => metrics.eden().add_local_cache_miss(labels),
                }
            }

            MetricEvent::RoleGranted { org_uuid, perms, resource, resource_id } => {
                let labels = Self::iam_labels(perms, *resource, *resource_id);
                let mut label_refs = labels.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>();
                label_refs.push(("org_uuid", org_uuid));
                metrics.iam().add_assignment(&label_refs);
            }

            MetricEvent::RoleRevoked { org_uuid, perms, resource, resource_id } => {
                let labels = Self::iam_labels(perms, *resource, *resource_id);
                let mut label_refs = labels.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>();
                label_refs.push(("org_uuid", org_uuid));
                metrics.iam().remove_assignment(&label_refs);
            }

            MetricEvent::RolesGrantedBatch { org_uuid, perms, count } => {
                let labels = Self::iam_labels(perms, None, None);
                let mut label_refs = labels.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>();
                label_refs.push(("org_uuid", org_uuid));
                metrics.iam().add_assignments(*count, &label_refs);
            }

            MetricEvent::ConnectionAcquired { org_uuid, db_type } => {
                if let Some(dt) = db_type {
                    let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("db_type", dt)];
                    metrics.eden().add_connection(labels);
                } else {
                    let labels: &[(&str, &str)] = &[("org_uuid", org_uuid)];
                    metrics.eden().add_connection(labels);
                }
            }

            // === Proxy Events ===
            MetricEvent::ProxyRequest { .. }
            | MetricEvent::NetworkLatency { .. }
            | MetricEvent::ProxyError { .. }
            | MetricEvent::ProxyConnectionFailure { .. } => {
                proxy::record_proxy_event(self, metrics);
            }

            // === Workload Profiling Events ===
            MetricEvent::WorkloadSnapshot { .. }
            | MetricEvent::WorkloadOpsPerSec { .. }
            | MetricEvent::WorkloadMemory { .. }
            | MetricEvent::WorkloadRatio { .. } => {
                workload::record_workload_event(self, metrics);
            }

            // === Snapshot Events ===
            MetricEvent::SnapshotStarted { .. }
            | MetricEvent::SnapshotCompleted { .. }
            | MetricEvent::SnapshotFailed { .. }
            | MetricEvent::SnapshotTargetWrite { .. }
            | MetricEvent::SnapshotBytesRead { .. }
            | MetricEvent::SnapshotBytesWritten { .. }
            | MetricEvent::SnapshotSchedulerPoll { .. }
            | MetricEvent::CdcWalPoll { .. }
            | MetricEvent::CdcFlush { .. }
            | MetricEvent::CdcBackfillCompleted { .. }
            | MetricEvent::CdcActiveWorkers { .. } => {
                snapshot::record_snapshot_event(self, metrics);
            }

            // === Metadata Collection Events ===
            MetricEvent::SlowlogEntriesCollected { .. }
            | MetricEvent::SlowlogAntipattern { .. }
            | MetricEvent::LatencyEvent { .. }
            | MetricEvent::KeyspaceNoTtlPercentage { .. }
            | MetricEvent::KeyspaceBigKeysCount { .. }
            | MetricEvent::PolicyBlocked { .. }
            | MetricEvent::PolicyWarned { .. } => {
                metadata::record_metadata_event(self, metrics);
            }
        }
    }
}

// Extension trait to add .record() to TelemetryWrapper
pub trait RecordMetric {
    fn record(&mut self, event: MetricEvent);
}

impl RecordMetric for TelemetryWrapper {
    #[inline]
    fn record(&mut self, event: MetricEvent) {
        event.record(self.metrics());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_metric_event_creation() {
        // Simple events
        let _login = MetricEvent::Login { org_uuid: "org_123" };
        let _cache_hit = MetricEvent::CacheHit { org_uuid: "tes-org", kind: CacheKind::Local };

        // Complex events
        let _role = MetricEvent::RoleGranted {
            org_uuid: "org_uuid",
            perms: ControlPerms::READ | ControlPerms::CONFIGURE | ControlPerms::GRANT,
            resource: Some("organization"),
            resource_id: Some("org_123"),
        };

        let _proxy_req = MetricEvent::ProxyRequest {
            org_uuid: "550e8400-e29b-41d4-a716-446655440231",
            interlay_uuid: "550e8400-e29b-41d4-a716-446655440000",
            endpoint_uuid: "550e8400-e29b-41d4-a716-446655440001",
            command_type: Some("GET"),
            duration_us: 45,
            bytes_read: 128,
            bytes_written: 256,
            command_count: 1,
        };

        let _network_latency = MetricEvent::NetworkLatency {
            org_uuid: "550e8400-e29b-41d4-a716-446655440231",
            endpoint_uuid: "550e8400-e29b-41d4-a716-446655440001",
            endpoint_kind: "redis",
            duration_us: 40,
        };

        // Workload profiling events
        let _workload_snapshot = MetricEvent::WorkloadSnapshot {
            org_uuid: "550e8400-e29b-41d4-a716-446655440231",
            endpoint_id: "redis_ep_1",
            avg_ops_per_sec: 1000.0,
            used_memory_bytes: 100 * 1024 * 1024, // 100 MB
            total_keys: 50000,
            keys_with_ttl: 25000,
            instantaneous_ops_per_sec: 1000,
            total_commands_processed: 1_000_000,
            used_cpu_user: 12.5,
            connected_clients: 42,
        };

        let _workload_ops = MetricEvent::WorkloadOpsPerSec {
            org_uuid: "550e8400-e29b-41d4-a716-446655440231",
            endpoint_id: "redis_ep_1",
            ops_per_sec: 1500.0,
        };

        let _workload_memory = MetricEvent::WorkloadMemory {
            org_uuid: "550e8400-e29b-41d4-a716-446655440231",
            endpoint_id: "redis_ep_1",
            memory_mb: 256.0,
        };

        let _workload_ratio = MetricEvent::WorkloadRatio {
            org_uuid: "550e8400-e29b-41d4-a716-446655440231",
            endpoint_id: "redis_ep_1",
            ratio: 5.86, // ops/sec / memory_mb
        };
    }

    #[test]
    fn local_cache_events_record_local_metrics_not_redis_metrics() {
        let metrics = Arc::new(AllMetrics::new());

        MetricEvent::CacheHit { org_uuid: "org_123", kind: CacheKind::Local }.record(&metrics);
        MetricEvent::CacheMiss { org_uuid: "org_123", kind: CacheKind::Local }.record(&metrics);

        assert_eq!(metrics.eden().get_local_cache_hits(), 1);
        assert_eq!(metrics.eden().get_local_cache_misses(), 1);
        assert_eq!(metrics.eden().get_redis_cache_hits(), 0);
        assert_eq!(metrics.eden().get_redis_cache_misses(), 0);
    }

    #[test]
    fn redis_cache_events_remain_on_legacy_metrics() {
        let metrics = Arc::new(AllMetrics::new());

        MetricEvent::CacheHit { org_uuid: "org_123", kind: CacheKind::Redis }.record(&metrics);
        MetricEvent::CacheMiss { org_uuid: "org_123", kind: CacheKind::Redis }.record(&metrics);

        assert_eq!(metrics.eden().get_local_cache_hits(), 0);
        assert_eq!(metrics.eden().get_local_cache_misses(), 0);
        assert_eq!(metrics.eden().get_redis_cache_hits(), 1);
        assert_eq!(metrics.eden().get_redis_cache_misses(), 1);
    }
}
