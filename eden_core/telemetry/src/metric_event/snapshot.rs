use crate::AllMetrics;
use std::sync::Arc;

/// Record snapshot-related metric events
pub(super) fn record_snapshot_event(event: &super::MetricEvent, metrics: &Arc<AllMetrics>) {
    use super::MetricEvent;

    match event {
        MetricEvent::SnapshotStarted { org_uuid, snapshot_id, source_endpoint_id, target_count } => {
            let target_count_str = target_count.to_string();
            let labels: &[(&str, &str)] = &[
                ("org_uuid", org_uuid),
                ("snapshot_id", snapshot_id),
                ("source_endpoint_uuid", source_endpoint_id),
                ("target_count", &target_count_str),
            ];
            metrics.snapshot().record_started(labels);
        }

        MetricEvent::SnapshotCompleted {
            org_uuid,
            snapshot_id,
            duration_secs,
            target_count,
            batches_total,
        } => {
            let target_count_str = target_count.to_string();
            let labels: &[(&str, &str)] = &[
                ("org_uuid", org_uuid),
                ("snapshot_id", snapshot_id),
                ("target_count", &target_count_str),
            ];
            metrics.snapshot().record_completed(labels);
            metrics.snapshot().record_duration(*duration_secs, labels);
            metrics.snapshot().record_batches_total(*batches_total, labels);
        }

        MetricEvent::SnapshotFailed { org_uuid, snapshot_id, duration_secs, error_type } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("snapshot_id", snapshot_id), ("error_type", error_type)];
            metrics.snapshot().record_failed(labels);
            metrics.snapshot().record_duration(*duration_secs, labels);
        }

        MetricEvent::SnapshotTargetWrite {
            org_uuid,
            snapshot_id,
            target_endpoint_id,
            success,
            duration_secs,
        } => {
            let result_str = if *success { "success" } else { "failure" };
            let labels: &[(&str, &str)] = &[
                ("org_uuid", org_uuid),
                ("snapshot_id", snapshot_id),
                ("target_endpoint_uuid", target_endpoint_id),
                ("result", result_str),
            ];
            metrics.snapshot().record_target_write(labels);
            metrics.snapshot().record_target_write_duration(*duration_secs, labels);
        }

        MetricEvent::SnapshotBytesRead { org_uuid, snapshot_id, bytes } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("snapshot_id", snapshot_id)];
            metrics.snapshot().record_bytes_read(*bytes, labels);
        }

        MetricEvent::SnapshotBytesWritten { org_uuid, snapshot_id, target_endpoint_id, bytes } => {
            let labels: &[(&str, &str)] = &[
                ("org_uuid", org_uuid),
                ("snapshot_id", snapshot_id),
                ("target_endpoint_uuid", target_endpoint_id),
            ];
            metrics.snapshot().record_bytes_written(*bytes, labels);
        }

        MetricEvent::SnapshotSchedulerPoll { org_uuid, snapshots_due } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid)];
            metrics.snapshot().record_scheduler_poll(*snapshots_due, labels);
        }

        // === CDC Events ===
        MetricEvent::CdcWalPoll { org_uuid, snapshot_id, rows_consumed } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("snapshot_id", snapshot_id)];
            metrics.snapshot().record_cdc_rows_consumed(*rows_consumed, labels);
        }

        MetricEvent::CdcFlush { org_uuid, snapshot_id, rows_written, duration_secs } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("snapshot_id", snapshot_id)];
            metrics.snapshot().record_cdc_rows_written(*rows_written, labels);
            metrics.snapshot().record_cdc_flush_duration(*duration_secs, labels);
        }

        MetricEvent::CdcBackfillCompleted { org_uuid, snapshot_id, rows_total, duration_secs } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid), ("snapshot_id", snapshot_id)];
            metrics.snapshot().record_cdc_backfill_rows(*rows_total, labels);
            metrics.snapshot().record_cdc_flush_duration(*duration_secs, labels);
        }

        MetricEvent::CdcActiveWorkers { org_uuid, count } => {
            let labels: &[(&str, &str)] = &[("org_uuid", org_uuid)];
            metrics.snapshot().record_cdc_active_workers(*count, labels);
        }

        _ => {}
    }
}
