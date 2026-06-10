use super::*;
impl OracleRedoLogInfo {
    /// Checks if log switching is happening too frequently
    pub fn is_switching_too_frequently(&self, threshold_per_hour: u64) -> bool {
        self.switches_last_hour > threshold_per_hour
    }

    /// Checks if archive lag is too high
    pub fn has_high_archive_lag(&self, threshold_seconds: f64) -> bool {
        self.archive_lag_seconds > threshold_seconds
    }

    /// Checks if there are pending archive operations
    pub fn has_pending_archives(&self) -> bool {
        self.pending_archive_count > 0
    }

    /// Checks if log buffer performance is poor
    pub fn has_poor_log_buffer_performance(&self, threshold_hit_ratio: f64) -> bool {
        self.log_buffer_hit_ratio < threshold_hit_ratio
    }

    /// Checks if redo write time is too high
    pub fn has_slow_redo_writes(&self, threshold_ms: f64) -> bool {
        self.avg_redo_write_time > threshold_ms
    }

    /// Checks if SCN gap is too large
    pub fn has_large_scn_gap(&self, threshold: u64) -> bool {
        self.scn_gap > threshold
    }

    /// Gets the estimated time until next log switch based on current generation rate
    pub fn estimated_time_to_next_switch(&self) -> f64 {
        if self.redo_generation_rate > 0.0 && self.log_file_size > 0 {
            let remaining_space =
                self.log_file_size as f64 * (1.0 - (self.time_since_last_switch * self.redo_generation_rate / self.log_file_size as f64));
            if remaining_space > 0.0 {
                remaining_space / self.redo_generation_rate
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Calculates the average redo generation rate in MB/hour
    pub fn redo_generation_rate_mb_per_hour(&self) -> f64 {
        self.redo_generation_rate * 3600.0 / (1024.0 * 1024.0)
    }

    /// Gets the current redo size in MB
    pub fn redo_size_today_mb(&self) -> f64 {
        bytes_to_mb(self.redo_size_today)
    }

    /// Gets log file size in MB
    pub fn log_file_size_mb(&self) -> f64 {
        bytes_to_mb(self.log_file_size)
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns a health summary based on various thresholds
    pub fn health_summary(&self) -> OracleRedoHealthSummary {
        OracleRedoHealthSummary {
            log_switch_health: status_by_count(self.switches_last_hour, 12, 24),
            archive_health: status_by_flags(
                self.has_high_archive_lag(300.0) || self.pending_archive_count > 0,
                self.has_high_archive_lag(600.0) || self.pending_archive_count > 5,
            ),
            performance_health: status_by_flags(
                self.has_poor_log_buffer_performance(95.0) || self.has_slow_redo_writes(20.0),
                self.has_poor_log_buffer_performance(90.0) || self.has_slow_redo_writes(50.0),
            ),
            scn_health: status_by_count(self.scn_gap, 500_000, 1_000_000),
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_redo_health_and_switch_estimate() {
        let mut redo_info = OracleRedoLogInfo {
            switches_last_hour: 15,
            archive_lag_seconds: 700.0,
            pending_archive_count: 3,
            log_buffer_hit_ratio: 92.0,
            avg_redo_write_time: 25.0,
            scn_gap: 750000,
            ..OracleRedoLogInfo::default()
        };

        let health = redo_info.health_summary();
        assert!(matches!(health.archive_health, RedoHealthStatus::Critical));
        assert!(matches!(health.performance_health, RedoHealthStatus::Warning));

        redo_info.redo_generation_rate = 1_000_000.0;
        redo_info.log_file_size = 100_000_000;
        redo_info.time_since_last_switch = 50.0;
        assert!((redo_info.estimated_time_to_next_switch() - 50.0).abs() < 0.1);
    }
}
