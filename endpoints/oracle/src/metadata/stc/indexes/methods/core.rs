use super::*;

impl OracleIndexInfo {
    pub fn index_size_human_readable(&self) -> String {
        Self::format_bytes(self.index_size_bytes)
    }

    pub fn format_bytes(bytes: u64) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", bytes, UNITS[unit_index])
        } else {
            format!("{size:.2} {}", UNITS[unit_index])
        }
    }

    pub fn is_effectively_used(&self) -> bool {
        self.usage_score > 0.0 && self.total_access_count > 0
    }

    pub fn has_performance_issues(&self) -> bool {
        self.fragmentation_level > 25.0 || self.blevel > 4 || (self.num_rows > 0 && self.clustering_factor > self.num_rows * 2)
    }

    pub fn health_status(&self) -> IndexHealthStatus {
        if self.status != "VALID" {
            return IndexHealthStatus::Invalid;
        }
        if self.needs_rebuild {
            return IndexHealthStatus::NeedsRebuild;
        }
        if self.drop_candidate {
            return IndexHealthStatus::DropCandidate;
        }
        if self.has_performance_issues() {
            return IndexHealthStatus::PerformanceIssues;
        }
        if self.stale_statistics {
            return IndexHealthStatus::StaleStats;
        }
        IndexHealthStatus::Healthy
    }

    pub fn maintenance_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.status != "VALID" {
            recommendations.push(format!("Rebuild index - status is {}", self.status));
        }

        if self.needs_rebuild {
            if let Some(reason) = &self.rebuild_reason {
                recommendations.push(format!("Rebuild index - {reason}"));
            } else {
                recommendations.push("Rebuild index - fragmentation detected".to_string());
            }
        }

        if self.stale_statistics {
            recommendations.push("Gather index statistics - statistics are stale".to_string());
        }

        if self.drop_candidate && self.uniqueness != "UNIQUE" {
            recommendations.push("Consider dropping index - not used and not enforcing uniqueness".to_string());
        }

        if self.blevel > 4 {
            recommendations.push(format!("Consider rebuilding - high B-tree depth ({})", self.blevel));
        }

        if self.num_rows > 0 && self.clustering_factor > self.num_rows * 2 {
            recommendations.push("Consider rebuilding - poor clustering factor".to_string());
        }

        if self.fragmentation_level > 50.0 {
            recommendations.push(format!("Rebuild recommended - high fragmentation ({:.1}%)", self.fragmentation_level));
        }

        if self.visibility == "INVISIBLE" {
            recommendations.push("Review invisible index - may need to be made visible or dropped".to_string());
        }

        if self.compression == "ENABLED" && self.prefix_length == 0 {
            recommendations.push("Review compression settings - prefix length is 0".to_string());
        }

        recommendations
    }

    pub fn rebuild_time_estimate(&self) -> RebuildTimeEstimate {
        match self.index_size_bytes {
            0..=104_857_600 => RebuildTimeEstimate::Fast,
            104_857_601..=1_073_741_824 => RebuildTimeEstimate::Medium,
            1_073_741_825..=10_737_418_240 => RebuildTimeEstimate::Slow,
            _ => RebuildTimeEstimate::VerySlow,
        }
    }

    pub fn is_compression_candidate(&self) -> bool {
        self.compression != "ENABLED"
            && self.index_size_bytes > 100_000_000
            && self.distinct_keys > 0
            && self.num_rows > 0
            && (self.num_rows / self.distinct_keys) > 10
    }

    pub fn space_efficiency_score(&self) -> f64 {
        if self.index_size_bytes == 0 || self.num_rows == 0 {
            return 0.0;
        }

        let bytes_per_row = self.index_size_bytes as f64 / self.num_rows as f64;
        let expected_bytes_per_row = self.column_count as f64 * 8.0;

        if bytes_per_row <= expected_bytes_per_row {
            100.0
        } else if bytes_per_row <= expected_bytes_per_row * 2.0 {
            75.0
        } else if bytes_per_row <= expected_bytes_per_row * 3.0 {
            50.0
        } else {
            25.0
        }
    }

    pub fn usage_frequency(&self) -> UsageFrequency {
        match self.usage_score as u32 {
            0 => UsageFrequency::Never,
            1..=20 => UsageFrequency::Rarely,
            21..=50 => UsageFrequency::Sometimes,
            51..=80 => UsageFrequency::Often,
            81..=100 => UsageFrequency::Frequently,
            _ => UsageFrequency::Never,
        }
    }

    pub fn supports_fast_full_scan(&self) -> bool {
        self.index_type == "NORMAL" && self.status == "VALID"
    }

    pub fn performance_impact_score(&self) -> f64 {
        let mut score = 100.0;

        if self.blevel > 2 {
            score -= (self.blevel - 2) as f64 * 10.0;
        }

        if self.num_rows > 0 && self.clustering_factor > self.num_rows {
            let clustering_ratio = self.clustering_factor as f64 / self.num_rows as f64;
            score -= (clustering_ratio - 1.0) * 20.0;
        }

        score -= self.fragmentation_level * 0.5;

        if self.selectivity < 0.1 {
            score -= 20.0;
        }

        score.clamp(0.0, 100.0)
    }
}
