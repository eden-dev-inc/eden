use super::*;
impl OracleSegmentInfo {
    /// Checks if overall space utilization is high
    pub fn is_space_utilization_high(&self, threshold_pct: f64) -> bool {
        self.space_utilization_pct > threshold_pct
    }

    /// Checks if there are many fragmented segments
    pub fn has_high_fragmentation(&self, threshold_count: u64) -> bool {
        self.fragmented_segments_count > threshold_count
    }

    /// Checks if growth activity exceeds the given threshold
    pub fn has_high_growth_activity(&self, threshold_count: u64) -> bool {
        self.growing_segments_count > threshold_count
    }

    /// Checks if there are tablespace space issues
    pub fn has_tablespace_issues(&self) -> bool {
        self.tablespaces_with_issues > 0
    }

    /// Checks if there are segments with high row chaining
    pub fn has_chaining_issues(&self, threshold_count: u64) -> bool {
        self.chained_segments_count > threshold_count
    }

    /// Gets the total allocated space in GB
    pub fn total_allocated_space_gb(&self) -> f64 {
        bytes_to_gb(self.total_allocated_space)
    }

    /// Gets the total used space in GB
    pub fn total_used_space_gb(&self) -> f64 {
        bytes_to_gb(self.total_used_space)
    }

    /// Gets the total free space in GB
    pub fn total_free_space_gb(&self) -> f64 {
        bytes_to_gb(self.total_free_space)
    }

    /// Gets the largest segment size in GB
    pub fn largest_segment_size_gb(&self) -> f64 {
        bytes_to_gb(self.largest_segment_size)
    }

    /// Gets the space allocated in last 24h in GB
    pub fn space_allocated_24h_gb(&self) -> f64 {
        bytes_to_gb(self.space_allocated_24h)
    }

    /// Gets the fragmentation waste in GB
    pub fn fragmentation_waste_gb(&self) -> f64 {
        bytes_to_gb(self.fragmentation_waste)
    }

    /// Gets the average segment size in MB
    pub fn avg_segment_size_mb(&self) -> f64 {
        if self.total_segments > 0 {
            bytes_to_mb(self.total_allocated_space) / self.total_segments as f64
        } else {
            0.0
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns a health summary based on various thresholds
    pub fn health_summary(&self) -> OracleSegmentHealthSummary {
        OracleSegmentHealthSummary {
            space_health: status_by_flags(
                self.is_space_utilization_high(85.0) || self.tablespace_utilization_pct > 85.0,
                self.is_space_utilization_high(95.0) || self.tablespace_utilization_pct > 95.0,
            ),
            fragmentation_health: status_by_count(self.fragmented_segments_count, 50, 100),
            growth_health: status_by_flags(self.has_high_growth_activity(50) || self.space_allocated_24h_gb() > 10.0, false),
            tablespace_health: status_by_count(self.tablespaces_with_issues, 0, 5),
            performance_health: status_by_count(self.chained_segments_count, 10, 20),
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_segment_health() {
        let segment_info = OracleSegmentInfo {
            total_segments: 500,
            total_allocated_space: 5_368_709_120,
            space_utilization_pct: 75.0,
            tablespace_utilization_pct: 92.0,
            fragmented_segments_count: 75,
            growing_segments_count: 30,
            chained_segments_count: 12,
            tablespaces_with_issues: 2,
            ..OracleSegmentInfo::default()
        };

        let health = segment_info.health_summary();
        assert!(matches!(health.space_health, SegmentHealthStatus::Warning));
        assert!(matches!(health.fragmentation_health, SegmentHealthStatus::Warning));

        assert!(segment_info.total_segments > 0);
    }
}
