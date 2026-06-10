use super::*;

impl ClickhouseStorageInfo {
    pub(super) fn generate_optimization_candidates(
        _core_info: &ClickhouseStorageInfo,
        detailed: &ClickhouseStorageDetailedMetrics,
    ) -> Vec<ClickhouseOptimizationCandidate> {
        let mut candidates = Vec::new();

        for fragmented in &detailed.fragmented_tables {
            candidates.push(ClickhouseOptimizationCandidate {
                database: fragmented.database.clone(),
                table_name: fragmented.table_name.clone(),
                optimization_type: OptimizationType::Defragmentation,
                current_issue: format!("Table has {} parts causing fragmentation", fragmented.parts_count),
                recommended_action: "Run OPTIMIZE TABLE to reduce part count".to_string(),
                expected_benefit: "Improved query performance and reduced storage overhead".to_string(),
                urgency: fragmented.optimization_urgency.clone(),
                estimated_time_minutes: Self::estimate_optimization_time(fragmented.total_size),
                potential_space_savings: fragmented.total_size / 10,
            });
        }

        for compressed in &detailed.poorly_compressed_tables {
            candidates.push(ClickhouseOptimizationCandidate {
                database: compressed.database.clone(),
                table_name: compressed.table_name.clone(),
                optimization_type: OptimizationType::CompressionImprovement,
                current_issue: format!("Poor compression ratio: {:.2}%", compressed.compression_ratio * 100.0),
                recommended_action: format!("Consider using {} compression codec", compressed.recommended_codec),
                expected_benefit: "Reduced storage usage and improved I/O performance".to_string(),
                urgency: if compressed.compression_ratio < 0.05 {
                    OptimizationUrgency::High
                } else {
                    OptimizationUrgency::Medium
                },
                estimated_time_minutes: Self::estimate_compression_optimization_time(compressed.total_bytes),
                potential_space_savings: compressed.potential_savings,
            });
        }

        candidates
    }

    pub(super) fn generate_efficiency_analysis(
        core_info: &ClickhouseStorageInfo,
        _detailed: &ClickhouseStorageDetailedMetrics,
    ) -> Vec<ClickhouseStorageEfficiencyAnalysis> {
        let mut analysis = Vec::new();

        analysis.push(ClickhouseStorageEfficiencyAnalysis {
            analysis_type: EfficiencyAnalysisType::Overall,
            metric_name: "Storage Compression Efficiency".to_string(),
            current_value: core_info.avg_compression_ratio,
            optimal_value: 0.15,
            efficiency_score: Self::calculate_compression_efficiency_score(core_info.avg_compression_ratio),
            recommendations: vec![
                "Consider using more aggressive compression codecs for large tables".to_string(),
                "Review data types to ensure optimal storage".to_string(),
                "Implement data archival policies for old data".to_string(),
            ],
            impact_level: if core_info.avg_compression_ratio > 0.5 {
                EfficiencyImpactLevel::High
            } else if core_info.avg_compression_ratio > 0.3 {
                EfficiencyImpactLevel::Medium
            } else {
                EfficiencyImpactLevel::Low
            },
        });

        if core_info.fragmented_tables > 0 {
            analysis.push(ClickhouseStorageEfficiencyAnalysis {
                analysis_type: EfficiencyAnalysisType::Fragmentation,
                metric_name: "Table Fragmentation Level".to_string(),
                current_value: core_info.fragmented_tables as f64,
                optimal_value: 0.0,
                efficiency_score: 1.0 - (core_info.fragmented_tables as f64 / core_info.total_tables as f64),
                recommendations: vec![
                    "Schedule regular OPTIMIZE TABLE operations".to_string(),
                    "Review partition strategies to reduce fragmentation".to_string(),
                    "Consider merge tree settings optimization".to_string(),
                ],
                impact_level: if core_info.fragmented_tables > core_info.total_tables / 4 {
                    EfficiencyImpactLevel::High
                } else {
                    EfficiencyImpactLevel::Medium
                },
            });
        }

        analysis
    }

    pub(super) fn calculate_potential_savings(uncompressed_bytes: u64, current_ratio: f64) -> u64 {
        let target_ratio = 0.15;
        if current_ratio > target_ratio {
            ((current_ratio - target_ratio) * uncompressed_bytes as f64) as u64
        } else {
            0
        }
    }

    pub(super) fn calculate_reclaimable_space(info: &ClickhouseStorageInfo) -> u64 {
        let inactive_parts = info.total_parts.saturating_sub(info.active_parts);
        let inactive_space = if info.total_parts > 0 {
            info.total_disk_usage.saturating_mul(inactive_parts) / info.total_parts
        } else {
            0
        };

        let fragmentation_space = if info.total_tables > 0 {
            info.total_disk_usage.saturating_mul(info.fragmented_tables).saturating_div(info.total_tables) / 4
        } else {
            0
        };

        inactive_space.saturating_add(fragmentation_space)
    }

    pub(super) fn calculate_optimization_needs(info: &ClickhouseStorageInfo) -> u64 {
        if info.total_tables == 0 {
            return 0;
        }

        let mut needs = info.fragmented_tables.saturating_add(info.poorly_compressed_tables);
        needs = needs.saturating_add(info.active_merges);
        if info.total_partitions > Self::LARGE_PARTITION_THRESHOLD {
            needs = needs.saturating_add(info.total_partitions - Self::LARGE_PARTITION_THRESHOLD);
        }

        needs.min(info.total_tables)
    }

    pub(super) fn get_recommended_compression_codec(engine: &str) -> String {
        match engine {
            "MergeTree" | "ReplacingMergeTree" | "SummingMergeTree" => "ZSTD(3)".to_string(),
            "AggregatingMergeTree" => "LZ4HC(9)".to_string(),
            _ => "ZSTD(1)".to_string(),
        }
    }

    pub(super) fn calculate_fragmentation_level(parts_count: u64) -> FragmentationLevel {
        if parts_count > 500 {
            FragmentationLevel::Critical
        } else if parts_count > 200 {
            FragmentationLevel::High
        } else if parts_count > 100 {
            FragmentationLevel::Medium
        } else {
            FragmentationLevel::Low
        }
    }

    pub(super) fn calculate_optimization_urgency(parts_count: u64, total_size: u64) -> OptimizationUrgency {
        let size_factor = if total_size > 107_374_182_400 { 2.0 } else { 1.0 };
        let urgency_score = (parts_count as f64 / 100.0) * size_factor;

        if urgency_score > 10.0 {
            OptimizationUrgency::Critical
        } else if urgency_score > 5.0 {
            OptimizationUrgency::High
        } else if urgency_score > 2.0 {
            OptimizationUrgency::Medium
        } else {
            OptimizationUrgency::Low
        }
    }

    pub(super) fn calculate_storage_efficiency(compression_ratio: f64, table_count: u64) -> f64 {
        let compression_score = if compression_ratio < 0.1 {
            1.0
        } else if compression_ratio < 0.3 {
            0.8
        } else {
            0.5
        };

        let scale_factor = if table_count > 100 { 0.9 } else { 1.0 };
        compression_score * scale_factor
    }

    pub(super) fn calculate_partition_health(parts_in_partition: u64, partition_size: u64) -> PartitionHealth {
        let parts_score = if parts_in_partition > 50 {
            0.0
        } else if parts_in_partition > 20 {
            0.5
        } else {
            1.0
        };
        let size_score = if partition_size > 10_737_418_240 { 0.5 } else { 1.0 };
        let overall_score = (parts_score + size_score) / 2.0;

        if overall_score > 0.8 {
            PartitionHealth::Healthy
        } else if overall_score > 0.5 {
            PartitionHealth::Warning
        } else {
            PartitionHealth::Critical
        }
    }

    pub(super) fn calculate_compression_efficiency_score(compression_ratio: f64) -> f64 {
        if compression_ratio < 0.1 {
            1.0
        } else if compression_ratio < 0.2 {
            0.8
        } else if compression_ratio < 0.3 {
            0.6
        } else if compression_ratio < 0.5 {
            0.4
        } else {
            0.2
        }
    }

    pub(super) fn estimate_optimization_time(total_size: u64) -> u64 {
        (total_size / 1_073_741_824).max(5)
    }

    pub(super) fn estimate_compression_optimization_time(total_bytes: u64) -> u64 {
        ((total_bytes / 1_073_741_824) * 2).max(10)
    }
}
