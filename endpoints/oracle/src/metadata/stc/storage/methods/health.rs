use super::*;

impl OracleStorageInfo {
    pub fn is_storage_utilization_high(&self, threshold_pct: f64) -> bool {
        self.storage_utilization_pct > threshold_pct
    }

    pub fn has_tablespace_issues(&self) -> bool {
        self.tablespaces_warning > 0 || self.tablespaces_critical > 0
    }

    pub fn has_file_limit_issues(&self) -> bool {
        self.files_near_maxsize > 0
    }

    pub fn has_significant_growth(&self, threshold_gb: f64) -> bool {
        self.storage_added_24h_gb() > threshold_gb
    }

    pub fn is_undo_utilization_high(&self, threshold_pct: f64) -> bool {
        self.undo_utilization_pct > threshold_pct
    }

    pub fn is_temp_utilization_high(&self, threshold_pct: f64) -> bool {
        self.temp_utilization_pct > threshold_pct
    }

    pub fn total_allocated_storage_gb(&self) -> f64 {
        bytes_to_gb(self.total_allocated_storage)
    }

    pub fn total_used_storage_gb(&self) -> f64 {
        bytes_to_gb(self.total_used_storage)
    }

    pub fn total_free_space_gb(&self) -> f64 {
        bytes_to_gb(self.total_free_space)
    }

    pub fn largest_tablespace_size_gb(&self) -> f64 {
        bytes_to_gb(self.largest_tablespace_size)
    }

    pub fn storage_added_24h_gb(&self) -> f64 {
        bytes_to_gb(self.storage_added_24h)
    }

    pub fn reclaimable_space_gb(&self) -> f64 {
        bytes_to_gb(self.reclaimable_space)
    }

    pub fn total_undo_space_gb(&self) -> f64 {
        bytes_to_gb(self.total_undo_space)
    }

    pub fn total_temp_space_gb(&self) -> f64 {
        bytes_to_gb(self.total_temp_space)
    }

    pub fn used_temp_space_gb(&self) -> f64 {
        bytes_to_gb(self.used_temp_space)
    }

    pub fn autoextend_percentage(&self) -> f64 {
        ratio_percentage(self.autoextend_data_files, self.total_data_files)
    }

    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    pub fn health_summary(&self) -> OracleStorageHealthSummary {
        OracleStorageHealthSummary {
            space_health: status_by_flags(
                self.tablespaces_warning > 0 || self.is_storage_utilization_high(85.0),
                self.tablespaces_critical > 0 || self.is_storage_utilization_high(95.0),
            ),
            growth_health: status_by_flags(
                self.has_significant_growth(5.0) || self.autoextend_events_24h > 20,
                self.has_significant_growth(10.0) || self.autoextend_events_24h > 50,
            ),
            file_health: status_by_count(self.files_near_maxsize, 0, 5),
            undo_health: status_by_high_threshold(self.undo_utilization_pct, 75.0, 90.0),
            temp_health: status_by_high_threshold(self.temp_utilization_pct, 50.0, 80.0),
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_storage_health() {
        let storage_info = OracleStorageInfo {
            total_tablespaces: 12,
            total_allocated_storage: 53_687_091_200,
            storage_utilization_pct: 75.0,
            tablespaces_warning: 1,
            tablespaces_critical: 0,
            total_data_files: 40,
            autoextend_data_files: 2,
            storage_added_24h: 2_147_483_648,
            autoextend_events_24h: 25,
            files_near_maxsize: 3,
            total_undo_space: 5_368_709_120,
            undo_utilization_pct: 65.0,
            total_temp_space: 2_147_483_648,
            temp_utilization_pct: 30.0,
            used_temp_space: 536_870_912,
            reclaimable_space: 268_435_456,
            ..OracleStorageInfo::default()
        };

        let health = storage_info.health_summary();
        assert!(matches!(health.space_health, StorageHealthStatus::Warning));
        assert!(matches!(health.growth_health, StorageHealthStatus::Warning));

        assert_eq!(storage_info.total_allocated_storage_gb(), 50.0);
        assert_eq!(storage_info.storage_added_24h_gb(), 2.0);
        assert_eq!(storage_info.total_undo_space_gb(), 5.0);
        assert_eq!(storage_info.reclaimable_space_gb(), 0.25);
        assert_eq!(storage_info.autoextend_percentage(), 5.0);
    }
}
