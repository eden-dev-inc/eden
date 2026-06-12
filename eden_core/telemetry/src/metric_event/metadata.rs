use crate::AllMetrics;
use crate::metric_event::MetricEvent;
use std::sync::Arc;

/// Record metadata-related metric events
pub fn record_metadata_event(event: &MetricEvent, metrics: &Arc<AllMetrics>) {
    match event {
        // === Slowlog Events ===
        MetricEvent::SlowlogEntriesCollected { org_uuid, endpoint_id, entries } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("endpoint_uuid", endpoint_id)];
            metrics.metadata().record_entries_collected(*entries, labels);
        }

        MetricEvent::SlowlogAntipattern { org_uuid, endpoint_id, pattern, count } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("endpoint_uuid", endpoint_id), ("pattern", pattern)];
            metrics.metadata().record_antipattern(*count, labels);
        }

        // === Latency Events ===
        MetricEvent::LatencyEvent { org_uuid, endpoint_id, event_name, latency_ms } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("endpoint_uuid", endpoint_id), ("event_name", event_name)];
            metrics.metadata().record_latency_event(*latency_ms, labels);
        }

        // === Keyspace Events ===
        MetricEvent::KeyspaceNoTtlPercentage { org_uuid, endpoint_id, percentage } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("endpoint_uuid", endpoint_id)];
            metrics.metadata().record_no_ttl_percentage(*percentage, labels);
        }

        MetricEvent::KeyspaceBigKeysCount { org_uuid, endpoint_id, count } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("endpoint_uuid", endpoint_id)];
            metrics.metadata().record_big_keys_count(*count, labels);
        }

        // === Policy Events ===
        MetricEvent::PolicyBlocked { org_uuid, endpoint_id, command, count } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("endpoint_uuid", endpoint_id), ("command", command)];
            metrics.metadata().record_policy_blocked(*count, labels);
        }

        MetricEvent::PolicyWarned { org_uuid, endpoint_id, command, count } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("endpoint_uuid", endpoint_id), ("command", command)];
            metrics.metadata().record_policy_warned(*count, labels);
        }

        _ => {} // Not a metadata event
    }
}
