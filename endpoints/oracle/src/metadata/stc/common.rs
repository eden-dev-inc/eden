use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

/// Shared health status enum for Oracle STC metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum HealthStatus {
    #[default]
    Healthy,
    Warning,
    Critical,
}

/// Converts bytes to gigabytes.
pub fn bytes_to_gb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0 * 1024.0)
}

/// Converts bytes to megabytes.
pub fn bytes_to_mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Returns a percentage for numerator/denominator as `0.0..=100.0`.
pub fn ratio_percentage(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 / denominator as f64) * 100.0
    }
}

/// Evaluates health where higher values are worse.
pub fn status_by_high_threshold(value: f64, warning_threshold: f64, critical_threshold: f64) -> HealthStatus {
    if value > critical_threshold {
        HealthStatus::Critical
    } else if value > warning_threshold {
        HealthStatus::Warning
    } else {
        HealthStatus::Healthy
    }
}

/// Evaluates health where lower values are worse.
pub fn status_by_low_threshold(value: f64, warning_threshold: f64, critical_threshold: f64) -> HealthStatus {
    if value < critical_threshold {
        HealthStatus::Critical
    } else if value < warning_threshold {
        HealthStatus::Warning
    } else {
        HealthStatus::Healthy
    }
}

/// Evaluates health from count thresholds where larger counts are worse.
pub fn status_by_count(value: u64, warning_threshold: u64, critical_threshold: u64) -> HealthStatus {
    if value > critical_threshold {
        HealthStatus::Critical
    } else if value > warning_threshold {
        HealthStatus::Warning
    } else {
        HealthStatus::Healthy
    }
}

/// Evaluates health from precomputed warning/critical flags.
pub fn status_by_flags(warning: bool, critical: bool) -> HealthStatus {
    if critical {
        HealthStatus::Critical
    } else if warning {
        HealthStatus::Warning
    } else {
        HealthStatus::Healthy
    }
}

#[macro_export]
macro_rules! impl_metadata_collection_boilerplate {
    ($description:expr, $category:expr, $interval:expr) => {
        fn description(&self) -> &'static str {
            $description
        }

        fn size(&self) -> usize {
            std::mem::size_of::<Self>()
        }

        fn category(&self) -> &'static str {
            $category
        }

        fn interval(&self) -> SyncFrequency {
            $interval
        }
    };
}
