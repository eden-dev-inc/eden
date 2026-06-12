use super::*;

impl ClickhouseStorageInfo {
    pub fn has_poor_compression(&self) -> bool {
        self.poorly_compressed_tables > 0 || self.avg_compression_ratio > 0.5
    }

    pub fn has_fragmented_tables(&self) -> bool {
        self.fragmented_tables > 0
    }

    pub fn has_active_merges(&self) -> bool {
        self.active_merges > 0
    }

    pub fn has_optimization_opportunities(&self) -> bool {
        self.tables_needing_optimization > 0
    }

    pub fn has_high_storage_usage(&self) -> bool {
        self.total_disk_usage > 1_099_511_627_776
    }

    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    pub fn get_storage_efficiency(&self) -> f64 {
        if self.total_uncompressed_size == 0 {
            return 1.0;
        }
        self.total_compressed_size as f64 / self.total_uncompressed_size as f64
    }

    pub fn get_fragmentation_ratio(&self) -> f64 {
        if self.total_tables == 0 {
            return 0.0;
        }
        self.fragmented_tables as f64 / self.total_tables as f64
    }

    pub fn get_poor_compression_ratio(&self) -> f64 {
        if self.total_tables == 0 {
            return 0.0;
        }
        self.poorly_compressed_tables as f64 / self.total_tables as f64
    }

    pub fn get_total_disk_usage_gb(&self) -> f64 {
        self.total_disk_usage as f64 / 1_073_741_824.0
    }

    pub fn get_avg_table_size_mb(&self) -> f64 {
        self.avg_table_size as f64 / 1_048_576.0
    }

    pub fn get_largest_table_size_gb(&self) -> f64 {
        self.largest_table_size as f64 / 1_073_741_824.0
    }

    pub fn get_reclaimable_space_gb(&self) -> f64 {
        self.reclaimable_space as f64 / 1_073_741_824.0
    }

    pub fn get_avg_parts_per_table(&self) -> f64 {
        if self.total_tables == 0 {
            return 0.0;
        }
        self.total_parts as f64 / self.total_tables as f64
    }

    pub fn get_storage_health_status(&self) -> StorageHealthStatus {
        let fragmentation_ratio = self.get_fragmentation_ratio();
        let compression_ratio = self.get_poor_compression_ratio();
        let avg_compression = self.avg_compression_ratio;

        if fragmentation_ratio > 0.3 || compression_ratio > 0.5 || avg_compression > 0.7 {
            StorageHealthStatus::Critical
        } else if fragmentation_ratio > 0.2 || compression_ratio > 0.3 || avg_compression > 0.5 {
            StorageHealthStatus::Warning
        } else if fragmentation_ratio > 0.1 || compression_ratio > 0.1 || avg_compression > 0.3 {
            StorageHealthStatus::Attention
        } else {
            StorageHealthStatus::Healthy
        }
    }

    pub fn get_storage_utilization_level(&self) -> StorageUtilizationLevel {
        let usage_tb = self.total_disk_usage as f64 / 1_099_511_627_776.0;

        if usage_tb > 50.0 {
            StorageUtilizationLevel::VeryHigh
        } else if usage_tb > 20.0 {
            StorageUtilizationLevel::High
        } else if usage_tb > 5.0 {
            StorageUtilizationLevel::Medium
        } else if usage_tb > 1.0 {
            StorageUtilizationLevel::Low
        } else {
            StorageUtilizationLevel::Minimal
        }
    }

    pub fn get_maintenance_burden(&self) -> StorageMaintenanceBurden {
        let total_issues = self.fragmented_tables + self.poorly_compressed_tables + self.tables_needing_optimization;

        if total_issues > self.total_tables / 2 {
            StorageMaintenanceBurden::VeryHigh
        } else if total_issues > self.total_tables / 4 {
            StorageMaintenanceBurden::High
        } else if total_issues > self.total_tables / 10 {
            StorageMaintenanceBurden::Medium
        } else if total_issues > 0 {
            StorageMaintenanceBurden::Low
        } else {
            StorageMaintenanceBurden::Minimal
        }
    }

    pub fn get_data_density(&self) -> f64 {
        let size_gb = self.get_total_disk_usage_gb();
        if size_gb == 0.0 {
            return 0.0;
        }
        self.total_rows as f64 / size_gb
    }

    pub fn get_compression_effectiveness(&self) -> f64 {
        if self.avg_compression_ratio <= 0.1 {
            1.0
        } else if self.avg_compression_ratio <= 0.2 {
            0.9
        } else if self.avg_compression_ratio <= 0.3 {
            0.7
        } else if self.avg_compression_ratio <= 0.5 {
            0.5
        } else if self.avg_compression_ratio <= 0.7 {
            0.3
        } else {
            0.1
        }
    }

    pub fn get_optimization_potential(&self) -> f64 {
        let fragmentation_factor = self.get_fragmentation_ratio();
        let compression_factor = self.get_poor_compression_ratio();
        let reclaimable_factor = if self.total_disk_usage > 0 {
            self.reclaimable_space as f64 / self.total_disk_usage as f64
        } else {
            0.0
        };

        ((fragmentation_factor + compression_factor + reclaimable_factor) / 3.0).min(1.0)
    }

    pub fn get_merge_activity_level(&self) -> MergeActivityLevel {
        if self.active_merges == 0 {
            MergeActivityLevel::Idle
        } else if self.active_merges <= 2 {
            MergeActivityLevel::Low
        } else if self.active_merges <= 5 {
            MergeActivityLevel::Moderate
        } else if self.active_merges <= 10 {
            MergeActivityLevel::High
        } else {
            MergeActivityLevel::VeryHigh
        }
    }

    pub fn get_space_efficiency_score(&self) -> f64 {
        let compression_score = self.get_compression_effectiveness();
        let fragmentation_penalty = self.get_fragmentation_ratio() * 0.3;
        let optimization_penalty = (self.tables_needing_optimization as f64 / self.total_tables.max(1) as f64) * 0.2;

        (compression_score - fragmentation_penalty - optimization_penalty).clamp(0.0, 1.0)
    }

    pub fn estimate_total_optimization_time_hours(&self) -> f64 {
        let fragmented_time = self.fragmented_tables as f64 * 0.5;
        let compression_time = self.poorly_compressed_tables as f64 * 2.0;
        let merge_time = self.active_merges as f64 * 0.25;

        (fragmented_time + compression_time + merge_time).max(0.0)
    }

    pub fn get_performance_impact_level(&self) -> StoragePerformanceImpact {
        let fragmentation_impact = self.get_fragmentation_ratio();
        let compression_impact = 1.0 - self.get_compression_effectiveness();
        let merge_impact = (self.active_merges as f64 / 20.0).min(1.0);

        let overall_impact = (fragmentation_impact + compression_impact + merge_impact) / 3.0;

        if overall_impact > 0.7 {
            StoragePerformanceImpact::Critical
        } else if overall_impact > 0.5 {
            StoragePerformanceImpact::High
        } else if overall_impact > 0.3 {
            StoragePerformanceImpact::Medium
        } else if overall_impact > 0.1 {
            StoragePerformanceImpact::Low
        } else {
            StoragePerformanceImpact::Minimal
        }
    }
}
