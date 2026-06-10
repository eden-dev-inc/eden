use super::*;

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct OracleIndexSummary {
    pub total_indexes: u64,
    pub healthy_indexes: u64,
    pub rebuild_needed: u64,
    pub unused_indexes: u64,
    pub stale_statistics: u64,
    pub total_index_size: u64,
    pub potential_space_savings: u64,
    pub avg_fragmentation_level: f64,
    pub avg_usage_score: f64,
    pub partitioned_indexes: u64,
    pub compressed_indexes: u64,
    pub unique_indexes: u64,
    pub invisible_indexes: u64,
}

#[allow(dead_code)]
impl OracleIndexSummary {
    pub fn from_indexes(indexes: &[OracleIndexInfo]) -> Self {
        let mut summary = OracleIndexSummary { total_indexes: indexes.len() as u64, ..Default::default() };

        for index in indexes {
            match index.health_status() {
                IndexHealthStatus::Healthy => summary.healthy_indexes += 1,
                IndexHealthStatus::NeedsRebuild => summary.rebuild_needed += 1,
                IndexHealthStatus::DropCandidate => summary.unused_indexes += 1,
                IndexHealthStatus::StaleStats => summary.stale_statistics += 1,
                _ => {}
            }

            summary.total_index_size += index.index_size_bytes;
            summary.potential_space_savings += index.rebuild_space_savings;

            if index.is_partitioned {
                summary.partitioned_indexes += 1;
            }
            if index.compression == "ENABLED" {
                summary.compressed_indexes += 1;
            }
            if index.uniqueness == "UNIQUE" {
                summary.unique_indexes += 1;
            }
            if index.visibility == "INVISIBLE" {
                summary.invisible_indexes += 1;
            }
        }

        if summary.total_indexes > 0 {
            summary.avg_fragmentation_level = indexes.iter().map(|i| i.fragmentation_level).sum::<f64>() / summary.total_indexes as f64;
            summary.avg_usage_score = indexes.iter().map(|i| i.usage_score).sum::<f64>() / summary.total_indexes as f64;
        }

        summary
    }

    pub fn total_index_size_human_readable(&self) -> String {
        OracleIndexInfo::format_bytes(self.total_index_size)
    }

    pub fn potential_space_savings_human_readable(&self) -> String {
        OracleIndexInfo::format_bytes(self.potential_space_savings)
    }

    pub fn healthy_percentage(&self) -> f64 {
        ratio_percentage(self.healthy_indexes, self.total_indexes)
    }

    pub fn needs_attention_percentage(&self) -> f64 {
        let needs_attention = self.rebuild_needed + self.stale_statistics;
        ratio_percentage(needs_attention, self.total_indexes)
    }
}
