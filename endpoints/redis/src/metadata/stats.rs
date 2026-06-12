// src/redis_metadata/stats.rs
//! Sync statistics tracking

use borsh::{BorshDeserialize, BorshSerialize};
use error::EpError;
use format::EdenNodeUuid;
use serde::{Deserialize, Serialize};
use telemetry::labels::TelemetryLabels;

/// Enhanced sync statistics that integrate with your telemetry system
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SyncStatistics {
    pub total_syncs: u64,
    pub successful_syncs: u64,
    pub failed_syncs: u64,
    pub last_sync_time: u64,
    pub last_sync_duration_ms: u64,
    pub categories_synced_last_run: Vec<String>,
    pub average_sync_duration_ms: f64,
    pub sync_errors: Vec<EpError>,
    pub metrics_labels: TelemetryLabels,
}

impl Default for SyncStatistics {
    fn default() -> Self {
        Self {
            total_syncs: 0,
            successful_syncs: 0,
            failed_syncs: 0,
            last_sync_time: 0,
            last_sync_duration_ms: 0,
            categories_synced_last_run: Vec::new(),
            average_sync_duration_ms: 0.0,
            sync_errors: Vec::new(),
            metrics_labels: TelemetryLabels::new(&EdenNodeUuid::new_uuid()),
        }
    }
}

impl SyncStatistics {
    /// Records a successful sync
    pub fn record_success(&mut self, duration_ms: u128, categories: Vec<String>) {
        self.total_syncs += 1;
        self.successful_syncs += 1;
        self.last_sync_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        self.last_sync_duration_ms = duration_ms as u64;
        self.categories_synced_last_run = categories;

        // Update rolling average
        let total_duration = self.average_sync_duration_ms * (self.successful_syncs - 1) as f64 + duration_ms as f64;
        self.average_sync_duration_ms = total_duration / self.successful_syncs as f64;

        // Keep only last 10 errors
        if self.sync_errors.len() > 10 {
            self.sync_errors.remove(0);
        }
    }

    /// Records a failed sync
    pub fn record_failure(&mut self, error: EpError) {
        self.total_syncs += 1;
        self.failed_syncs += 1;
        self.sync_errors.push(error);

        // Keep only last 10 errors
        if self.sync_errors.len() > 10 {
            self.sync_errors.remove(0);
        }
    }

    /// Gets the success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_syncs == 0 {
            0.0
        } else {
            (self.successful_syncs as f64 / self.total_syncs as f64) * 100.0
        }
    }
}
