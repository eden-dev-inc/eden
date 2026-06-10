//! Infrastructure snapshot row types for ClickHouse analytics.
//!
//! Metrics for data snapshot (fan-out copy) operations.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::Serialize;

/// Row for analytics.infrastructure_snapshots.
#[derive(Debug, Clone, Serialize, Row)]
pub struct InfrastructureSnapshotRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub snapshot_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub snapshot_uuid: String,
    pub source_endpoint_uuid: String,
    pub status: String,
    pub error_type: Option<String>,
    pub target_count: u32,
    pub batches_total: u64,
    pub duration_secs: f64,
    pub bytes_written_total: u64,
    pub target_writes_success: u64,
    pub target_writes_failure: u64,
    pub is_scheduler_poll: u8,
    pub scheduler_snapshots_due: u32,
}

impl InfrastructureSnapshotRow {
    /// Create a row for a snapshot started event.
    pub fn started(organization_uuid: String, snapshot_uuid: String, source_endpoint_uuid: String, target_count: u32) -> Self {
        Self {
            snapshot_time: Utc::now(),
            organization_uuid,
            snapshot_uuid,
            source_endpoint_uuid,
            status: "started".to_string(),
            error_type: None,
            target_count,
            batches_total: 0,
            duration_secs: 0.0,
            bytes_written_total: 0,
            target_writes_success: 0,
            target_writes_failure: 0,
            is_scheduler_poll: 0,
            scheduler_snapshots_due: 0,
        }
    }

    /// Create a row for a snapshot completed event.
    pub fn completed(
        organization_uuid: String,
        snapshot_uuid: String,
        source_endpoint_uuid: String,
        target_count: u32,
        duration_secs: f64,
        batches_total: u64,
    ) -> Self {
        Self::completed_with_metrics(
            organization_uuid,
            snapshot_uuid,
            source_endpoint_uuid,
            target_count,
            duration_secs,
            batches_total,
            0,
            0,
            0,
        )
    }

    /// Create a row for a snapshot completed event with write metrics.
    #[allow(clippy::too_many_arguments)]
    pub fn completed_with_metrics(
        organization_uuid: String,
        snapshot_uuid: String,
        source_endpoint_uuid: String,
        target_count: u32,
        duration_secs: f64,
        batches_total: u64,
        bytes_written_total: u64,
        target_writes_success: u64,
        target_writes_failure: u64,
    ) -> Self {
        Self {
            snapshot_time: Utc::now(),
            organization_uuid,
            snapshot_uuid,
            source_endpoint_uuid,
            status: "completed".to_string(),
            error_type: None,
            target_count,
            batches_total,
            duration_secs,
            bytes_written_total,
            target_writes_success,
            target_writes_failure,
            is_scheduler_poll: 0,
            scheduler_snapshots_due: 0,
        }
    }

    /// Create a row for a snapshot failed event.
    pub fn failed(
        organization_uuid: String,
        snapshot_uuid: String,
        source_endpoint_uuid: String,
        duration_secs: f64,
        error_type: String,
    ) -> Self {
        Self::failed_with_metrics(organization_uuid, snapshot_uuid, source_endpoint_uuid, duration_secs, error_type, 0, 0, 0)
    }

    /// Create a row for a snapshot failed event with write metrics.
    #[allow(clippy::too_many_arguments)]
    pub fn failed_with_metrics(
        organization_uuid: String,
        snapshot_uuid: String,
        source_endpoint_uuid: String,
        duration_secs: f64,
        error_type: String,
        bytes_written_total: u64,
        target_writes_success: u64,
        target_writes_failure: u64,
    ) -> Self {
        Self {
            snapshot_time: Utc::now(),
            organization_uuid,
            snapshot_uuid,
            source_endpoint_uuid,
            status: "failed".to_string(),
            error_type: Some(error_type),
            target_count: 0,
            batches_total: 0,
            duration_secs,
            bytes_written_total,
            target_writes_success,
            target_writes_failure,
            is_scheduler_poll: 0,
            scheduler_snapshots_due: 0,
        }
    }

    /// Create a row for a scheduler poll event.
    pub fn scheduler_poll(organization_uuid: String, snapshots_due: u32) -> Self {
        Self {
            snapshot_time: Utc::now(),
            organization_uuid,
            snapshot_uuid: String::new(),
            source_endpoint_uuid: String::new(),
            status: "scheduler_poll".to_string(),
            error_type: None,
            target_count: 0,
            batches_total: 0,
            duration_secs: 0.0,
            bytes_written_total: 0,
            target_writes_success: 0,
            target_writes_failure: 0,
            is_scheduler_poll: 1,
            scheduler_snapshots_due: snapshots_due,
        }
    }
}

/// Table name for infrastructure snapshots.
pub const INFRASTRUCTURE_SNAPSHOTS_TABLE: &str = "analytics.infrastructure_snapshots";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_started_row() {
        let row = InfrastructureSnapshotRow::started("test-org".to_string(), "snap-123".to_string(), "ep-source".to_string(), 3);
        assert_eq!(row.status, "started");
        assert_eq!(row.target_count, 3);
        assert!(row.error_type.is_none());
    }

    #[test]
    fn test_completed_row() {
        let row =
            InfrastructureSnapshotRow::completed("test-org".to_string(), "snap-123".to_string(), "ep-source".to_string(), 3, 45.5, 100);
        assert_eq!(row.status, "completed");
        assert_eq!(row.duration_secs, 45.5);
        assert_eq!(row.batches_total, 100);
    }

    #[test]
    fn test_failed_row() {
        let row = InfrastructureSnapshotRow::failed(
            "test-org".to_string(),
            "snap-123".to_string(),
            "ep-source".to_string(),
            10.0,
            "connection_timeout".to_string(),
        );
        assert_eq!(row.status, "failed");
        assert_eq!(row.error_type, Some("connection_timeout".to_string()));
    }

    #[test]
    fn test_scheduler_poll_row() {
        let row = InfrastructureSnapshotRow::scheduler_poll("test-org".to_string(), 5);
        assert_eq!(row.status, "scheduler_poll");
        assert_eq!(row.is_scheduler_poll, 1);
        assert_eq!(row.scheduler_snapshots_due, 5);
    }
}
