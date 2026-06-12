use crate::api::lib::QueryInput;
use borsh::{BorshDeserialize, BorshSerialize};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

mod analytics;
mod collection_impl;
mod core_sync;
mod detailed_sync;
mod helpers;
mod model_types;
mod parsers;

pub(crate) use model_types::*;

/// Clickhouse storage and data management metrics.
///
/// Covers disk usage, table sizes, compression ratios and growth patterns.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseStorageInfo {
    /// Total disk space used by all databases in bytes
    pub total_disk_usage: u64,
    /// Total number of tables across all databases
    pub total_tables: u64,
    /// Total number of data parts across all tables
    pub total_parts: u64,
    /// Total number of rows across all tables
    pub total_rows: u64,
    /// Total number of databases
    pub total_databases: u64,
    /// Number of active data parts (not being merged)
    pub active_parts: u64,
    /// Number of inactive data parts
    pub inactive_parts: u64,
    /// Total uncompressed data size in bytes
    pub total_uncompressed_size: u64,
    /// Total compressed data size in bytes
    pub total_compressed_size: u64,
    /// Average compression ratio across all data
    pub avg_compression_ratio: f64,
    /// Number of tables with poor compression ratio
    pub poorly_compressed_tables: u64,
    /// Number of tables with fragmentation issues
    pub fragmented_tables: u64,
    /// Total number of merges in progress
    pub active_merges: u64,
    /// Number of failed merge operations in last hour
    pub failed_merges_last_hour: u64,
    /// Total storage space that could be reclaimed
    pub reclaimable_space: u64,
    /// Number of tables requiring optimization
    pub tables_needing_optimization: u64,
    /// Average table size in bytes
    pub avg_table_size: u64,
    /// Largest table size in bytes
    pub largest_table_size: u64,
    /// Number of partitions across all tables
    pub total_partitions: u64,
    /// Detailed storage metrics collected when issues are detected
    pub detailed_metrics: Option<ClickhouseStorageDetailedMetrics>,
}

/// Detailed storage metrics collected when storage issues are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseStorageDetailedMetrics {
    /// Tables with largest disk usage
    pub largest_tables: Vec<ClickhouseLargeTable>,
    /// Tables with poor compression ratios
    pub poorly_compressed_tables: Vec<ClickhousePoorCompressionTable>,
    /// Tables with high fragmentation
    pub fragmented_tables: Vec<ClickhouseFragmentedTable>,
    /// Currently active merge operations
    pub active_merges: Vec<ClickhouseActiveMerge>,
    /// Recent failed merge operations
    pub failed_merges: Vec<ClickhouseFailedMerge>,
    /// Tables that need optimization
    pub optimization_candidates: Vec<ClickhouseOptimizationCandidate>,
    /// Disk usage by database
    pub database_storage_stats: Vec<ClickhouseDatabaseStorageStats>,
    /// Storage growth trends
    pub growth_trends: Vec<ClickhouseStorageGrowthTrend>,
    /// Partition management information
    pub partition_info: Vec<ClickhousePartitionInfo>,
    /// Storage efficiency analysis
    pub efficiency_analysis: Vec<ClickhouseStorageEfficiencyAnalysis>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoint_types::metadata::MetadataCollection;

    #[test]
    fn test_clickhouse_storage_calculations() {
        let storage_info = ClickhouseStorageInfo {
            total_disk_usage: 2_199_023_255_552,
            total_tables: 100,
            total_parts: 5000,
            total_rows: 1_000_000_000,
            total_databases: 5,
            active_parts: 4800,
            inactive_parts: 200,
            total_uncompressed_size: 10_995_116_277_760,
            total_compressed_size: 2_199_023_255_552,
            avg_compression_ratio: 0.2,
            poorly_compressed_tables: 15,
            fragmented_tables: 8,
            active_merges: 3,
            avg_table_size: 21_474_836_480,
            largest_table_size: 107_374_182_400,
            total_partitions: 500,
            reclaimable_space: 109_951_162_777,
            tables_needing_optimization: 20,
            ..Default::default()
        };

        assert_eq!(storage_info.get_storage_efficiency(), 0.2);
        assert_eq!(storage_info.get_fragmentation_ratio(), 0.08);
        assert_eq!(storage_info.get_poor_compression_ratio(), 0.15);
        assert!((storage_info.get_total_disk_usage_gb() - 2048.0).abs() < 10.0);
        assert!((storage_info.get_avg_table_size_mb() - 20480.0).abs() < 100.0);
        assert!((storage_info.get_largest_table_size_gb() - 100.0).abs() < 1.0);
        assert!((storage_info.get_reclaimable_space_gb() - 102.4).abs() < 5.0);
        assert_eq!(storage_info.get_avg_parts_per_table(), 50.0);

        let data_density = storage_info.get_data_density();
        assert!(data_density > 0.0);

        assert!(storage_info.has_poor_compression());
        assert!(storage_info.has_fragmented_tables());
        assert!(storage_info.has_active_merges());
        assert!(storage_info.has_optimization_opportunities());
        assert!(storage_info.has_high_storage_usage());

        let health_status = storage_info.get_storage_health_status();
        assert!(matches!(health_status, StorageHealthStatus::Attention));

        let utilization_level = storage_info.get_storage_utilization_level();
        assert!(matches!(utilization_level, StorageUtilizationLevel::Low));

        let maintenance_burden = storage_info.get_maintenance_burden();
        assert!(matches!(maintenance_burden, StorageMaintenanceBurden::High));

        let compression_effectiveness = storage_info.get_compression_effectiveness();
        assert!(compression_effectiveness > 0.5 && compression_effectiveness < 1.0);

        let optimization_potential = storage_info.get_optimization_potential();
        assert!(optimization_potential > 0.0 && optimization_potential < 1.0);

        let merge_activity = storage_info.get_merge_activity_level();
        assert!(matches!(merge_activity, MergeActivityLevel::Moderate));

        let space_efficiency = storage_info.get_space_efficiency_score();
        assert!((0.0..=1.0).contains(&space_efficiency));

        let optimization_time = storage_info.estimate_total_optimization_time_hours();
        assert!(optimization_time > 0.0);

        let performance_impact = storage_info.get_performance_impact_level();
        assert!(matches!(performance_impact, StoragePerformanceImpact::Low | StoragePerformanceImpact::Medium));
    }

    #[test]
    fn test_storage_health_classification() {
        // Test healthy status
        let healthy_storage = ClickhouseStorageInfo {
            total_tables: 50,
            fragmented_tables: 2,        // 4% fragmentation
            poorly_compressed_tables: 3, // 6% poor compression
            avg_compression_ratio: 0.15, // Good compression
            tables_needing_optimization: 5,
            ..Default::default()
        };

        assert!(matches!(healthy_storage.get_storage_health_status(), StorageHealthStatus::Healthy));

        // Test critical status
        let critical_storage = ClickhouseStorageInfo {
            total_tables: 50,
            fragmented_tables: 20,        // 40% fragmentation
            poorly_compressed_tables: 30, // 60% poor compression
            avg_compression_ratio: 0.8,   // Very poor compression
            tables_needing_optimization: 35,
            ..Default::default()
        };

        assert!(matches!(critical_storage.get_storage_health_status(), StorageHealthStatus::Critical));
    }

    #[test]
    fn test_fragmentation_level_calculation() {
        assert!(matches!(ClickhouseStorageInfo::calculate_fragmentation_level(50), FragmentationLevel::Low));

        assert!(matches!(ClickhouseStorageInfo::calculate_fragmentation_level(150), FragmentationLevel::Medium));

        assert!(matches!(ClickhouseStorageInfo::calculate_fragmentation_level(300), FragmentationLevel::High));

        assert!(matches!(ClickhouseStorageInfo::calculate_fragmentation_level(700), FragmentationLevel::Critical));
    }

    #[test]
    fn test_optimization_urgency_calculation() {
        assert!(matches!(
            ClickhouseStorageInfo::calculate_optimization_urgency(50, 1_073_741_824), // 1GB, 50 parts
            OptimizationUrgency::Low
        ));

        assert!(matches!(
            ClickhouseStorageInfo::calculate_optimization_urgency(300, 53_687_091_200), // 50GB, 300 parts
            OptimizationUrgency::Medium
        ));

        assert!(matches!(
            ClickhouseStorageInfo::calculate_optimization_urgency(800, 214_748_364_800), // 200GB, 800 parts
            OptimizationUrgency::Critical
        ));
    }

    #[test]
    fn test_potential_savings_calculation() {
        // Test with poor compression (50% ratio, target 15%)
        let savings = ClickhouseStorageInfo::calculate_potential_savings(1_000_000_000, 0.5);
        assert_eq!(savings, 350_000_000); // 35% savings

        // Test with good compression (10% ratio, already better than target)
        let no_savings = ClickhouseStorageInfo::calculate_potential_savings(1_000_000_000, 0.1);
        assert_eq!(no_savings, 0);
    }

    #[test]
    fn test_compression_effectiveness_score() {
        assert_eq!(ClickhouseStorageInfo::calculate_compression_efficiency_score(0.05), 1.0);
        assert_eq!(ClickhouseStorageInfo::calculate_compression_efficiency_score(0.15), 0.8);
        assert_eq!(ClickhouseStorageInfo::calculate_compression_efficiency_score(0.25), 0.6);
        assert_eq!(ClickhouseStorageInfo::calculate_compression_efficiency_score(0.8), 0.2);
    }

    #[test]
    fn test_storage_request_uses_named_keys() {
        let req = ClickhouseStorageInfo::default().request();

        assert!(req.contains_key(ClickhouseStorageInfo::QUERY_STORAGE_OVERVIEW));
        assert!(req.contains_key(ClickhouseStorageInfo::QUERY_PARTS_OVERVIEW));
        assert!(req.contains_key(ClickhouseStorageInfo::QUERY_TABLE_SIZES));
        assert!(req.contains_key(ClickhouseStorageInfo::QUERY_MERGE_OPERATIONS));
        assert!(req.contains_key(ClickhouseStorageInfo::QUERY_FRAGMENTATION_CHECK));
        assert!(req.contains_key(ClickhouseStorageInfo::QUERY_PARTITION_STATS));
    }

    #[test]
    fn test_reclaimable_and_optimization_needs_calculation() {
        let info = ClickhouseStorageInfo {
            total_disk_usage: 1_000_000_000,
            total_tables: 100,
            total_parts: 1_000,
            active_parts: 800,
            fragmented_tables: 10,
            poorly_compressed_tables: 5,
            active_merges: 2,
            total_partitions: 1_200,
            ..Default::default()
        };

        assert!(ClickhouseStorageInfo::calculate_reclaimable_space(&info) > 0);
        assert!(ClickhouseStorageInfo::calculate_optimization_needs(&info) >= 17);
    }
}
