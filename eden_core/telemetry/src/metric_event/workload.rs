use crate::AllMetrics;
use crate::metrics::workload::WorkloadSnapshot;
use std::sync::Arc;

/// Record workload-related metric events
pub(super) fn record_workload_event(event: &super::MetricEvent, metrics: &Arc<AllMetrics>) {
    use super::MetricEvent;

    match event {
        MetricEvent::WorkloadSnapshot {
            org_uuid,
            endpoint_id,
            avg_ops_per_sec,
            used_memory_bytes,
            total_keys,
            keys_with_ttl,
            instantaneous_ops_per_sec,
            total_commands_processed,
            used_cpu_user,
            connected_clients,
        } => {
            let snapshot = WorkloadSnapshot::new(
                endpoint_id.to_string(),
                *avg_ops_per_sec,
                *used_memory_bytes,
                *total_keys,
                *keys_with_ttl,
                *instantaneous_ops_per_sec,
                *total_commands_processed,
                *used_cpu_user,
                *connected_clients,
            );

            let amr_profile_str = snapshot.amr_profile.as_str();
            let labels: &[(&str, &str)] = &[
                ("org_uuid", org_uuid),
                ("endpoint_uuid", endpoint_id),
                ("amr_profile", amr_profile_str),
            ];
            metrics.workload().record_snapshot(&snapshot, labels);
        }

        MetricEvent::WorkloadOpsPerSec { org_uuid, endpoint_id, ops_per_sec } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("endpoint_uuid", endpoint_id)];
            metrics.workload().record_ops_per_sec(*ops_per_sec, labels);
        }

        MetricEvent::WorkloadMemory { org_uuid, endpoint_id, memory_mb } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("endpoint_uuid", endpoint_id)];
            metrics.workload().record_memory(*memory_mb, labels);
        }

        MetricEvent::WorkloadRatio { org_uuid, endpoint_id, ratio } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("endpoint_uuid", endpoint_id)];
            metrics.workload().record_ratio(*ratio, labels);
        }

        _ => {
            // This should never happen since we pattern match all workload events in mod.rs
        }
    }
}
