use super::*;
impl OracleTablespaceInfo {
    /// Checks if any tablespaces are critically full
    pub fn has_critical_usage(&self) -> bool {
        self.critical_usage_tablespaces > 0
    }

    /// Checks if any tablespaces have high usage
    pub fn has_high_usage(&self, threshold_count: u64) -> bool {
        self.high_usage_tablespaces > threshold_count
    }

    /// Checks if there are offline tablespaces
    pub fn has_offline_tablespaces(&self) -> bool {
        self.offline_tablespaces > 0
    }

    /// Gets overall usage percentage
    pub fn overall_usage_percent(&self) -> f64 {
        if self.total_allocated_bytes > 0 {
            (self.total_used_bytes as f64 / self.total_allocated_bytes as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Gets total allocated space in GB
    pub fn total_allocated_gb(&self) -> f64 {
        bytes_to_gb(self.total_allocated_bytes)
    }

    /// Gets total used space in GB
    pub fn total_used_gb(&self) -> f64 {
        bytes_to_gb(self.total_used_bytes)
    }

    /// Gets total free space in GB
    pub fn total_free_gb(&self) -> f64 {
        bytes_to_gb(self.total_free_bytes)
    }

    /// Gets total maximum space in GB
    pub fn total_max_gb(&self) -> f64 {
        bytes_to_gb(self.total_max_bytes)
    }

    /// Gets autoextend coverage percentage
    pub fn autoextend_coverage(&self) -> f64 {
        ratio_percentage(self.autoextend_datafiles, self.total_datafiles)
    }

    /// Gets bigfile tablespace percentage
    pub fn bigfile_percentage(&self) -> f64 {
        ratio_percentage(self.bigfile_tablespaces, self.total_tablespaces)
    }

    /// Gets locally managed percentage
    pub fn locally_managed_percentage(&self) -> f64 {
        ratio_percentage(self.locally_managed, self.total_tablespaces)
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns a health summary based on various thresholds
    pub fn health_summary(&self) -> OracleTablespaceHealthSummary {
        OracleTablespaceHealthSummary {
            usage_health: status_by_flags(self.high_usage_tablespaces > 2, self.critical_usage_tablespaces > 0),
            availability_health: status_by_flags(false, self.offline_tablespaces > 0),
            autoextend_health: status_by_flags(self.autoextend_coverage() < 50.0 && self.high_usage_tablespaces > 0, false),
            management_health: status_by_low_threshold(self.locally_managed_percentage(), 80.0, 0.0),
        }
    }
}
