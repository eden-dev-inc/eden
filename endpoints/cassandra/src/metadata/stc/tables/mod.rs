use crate::api::lib::QueryUnpagedInput;
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use std::collections::HashMap;

mod collector;
mod types;

use super::utils;

pub use types::*;

impl MetadataCollection for CassandraTableInfo {
    type Request = HashMap<String, QueryUnpagedInput>;

    fn request(&self) -> Self::Request {
        collector::build_request()
    }

    fn description(&self) -> &'static str {
        "Cassandra table information and analytics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "table"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collection_type_detection() {
        assert!(CassandraTableInfo::is_collection_type("map<text, int>"));
        assert!(CassandraTableInfo::is_collection_type("set<uuid>"));
        assert!(CassandraTableInfo::is_collection_type("list<text>"));
        assert!(!CassandraTableInfo::is_collection_type("text"));
        assert!(!CassandraTableInfo::is_collection_type("int"));
    }

    #[test]
    fn test_primitive_type_detection() {
        assert!(CassandraTableInfo::is_primitive_type("text"));
        assert!(CassandraTableInfo::is_primitive_type("int"));
        assert!(CassandraTableInfo::is_primitive_type("uuid"));
        assert!(CassandraTableInfo::is_primitive_type("timestamp"));
        assert!(!CassandraTableInfo::is_primitive_type("user_type"));
        assert!(!CassandraTableInfo::is_primitive_type("map<text, int>"));
    }

    #[test]
    fn test_udt_type_detection() {
        assert!(!CassandraTableInfo::is_udt_type("text"));
        assert!(!CassandraTableInfo::is_udt_type("map<text, int>"));
        assert!(CassandraTableInfo::is_udt_type("user_defined_type"));
        assert!(CassandraTableInfo::is_udt_type("address_info"));
    }

    #[test]
    fn test_key_complexity_calculation() {
        let simple_key = vec![CassandraTableColumn {
            name: "id".to_string(),
            data_type: "uuid".to_string(),
            kind: "partition_key".to_string(),
            position: Some(0),
            clustering_order: None,
            is_collection: false,
            is_udt: false,
        }];
        assert_eq!(CassandraTableInfo::calculate_key_complexity(&simple_key), 1.0);

        let complex_key = vec![
            CassandraTableColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                kind: "partition_key".to_string(),
                position: Some(0),
                clustering_order: None,
                is_collection: false,
                is_udt: false,
            },
            CassandraTableColumn {
                name: "data".to_string(),
                data_type: "map<text, text>".to_string(),
                kind: "partition_key".to_string(),
                position: Some(1),
                clustering_order: None,
                is_collection: true,
                is_udt: false,
            },
        ];
        // 2 columns + 2 for collection
        assert_eq!(CassandraTableInfo::calculate_key_complexity(&complex_key), 4.0);
    }

    #[test]
    fn test_system_keyspace_detection() {
        assert!(CassandraTableInfo::is_system_keyspace("system"));
        assert!(CassandraTableInfo::is_system_keyspace("system_schema"));
        assert!(CassandraTableInfo::is_system_keyspace("system_auth"));
        assert!(!CassandraTableInfo::is_system_keyspace("my_keyspace"));
        assert!(!CassandraTableInfo::is_system_keyspace("user_data"));
    }

    #[test]
    fn test_critical_issues_detection() {
        let mut table_info = CassandraTableInfo::default();

        table_info.health_metrics.overall_health_score = 80.0;
        table_info.total_tables = 10;
        table_info.tables_with_issues = 2;
        table_info.maintenance_metrics.tables_needing_maintenance = 1;
        assert!(!table_info.has_critical_table_issues());

        table_info.health_metrics.overall_health_score = 50.0;
        assert!(table_info.has_critical_table_issues());

        table_info.health_metrics.overall_health_score = 80.0;
        table_info.tables_with_issues = 6; // More than half
        assert!(table_info.has_critical_table_issues());

        table_info.tables_with_issues = 2;
        table_info.maintenance_metrics.tables_needing_maintenance = 4; // More than 1/3
        assert!(table_info.has_critical_table_issues());
    }

    #[test]
    fn test_health_rating() {
        let mut table_info = CassandraTableInfo::default();

        table_info.health_metrics.overall_health_score = 95.0;
        assert_eq!(table_info.table_health_rating(), "A");

        table_info.health_metrics.overall_health_score = 85.0;
        assert_eq!(table_info.table_health_rating(), "B");

        table_info.health_metrics.overall_health_score = 75.0;
        assert_eq!(table_info.table_health_rating(), "C");

        table_info.health_metrics.overall_health_score = 65.0;
        assert_eq!(table_info.table_health_rating(), "D");

        table_info.health_metrics.overall_health_score = 45.0;
        assert_eq!(table_info.table_health_rating(), "F");
    }

    #[test]
    fn test_storage_efficiency_calculation() {
        let mut table_info = CassandraTableInfo::default();

        // No tables
        assert_eq!(table_info.storage_efficiency_score(), 100.0);

        let good_table = CassandraTableDetail {
            keyspace_name: "test".to_string(),
            table_name: "good_table".to_string(),
            table_id: "123".to_string(),
            table_type: "USER".to_string(),
            column_info: CassandraTableColumnInfo::default(),
            storage_metrics: CassandraTableStorageMetrics { compression_ratio: 5.0, ..Default::default() },
            performance_metrics: CassandraTablePerformanceDetail::default(),
            configuration: CassandraTableConfiguration::default(),
            indexes: vec![],
            materialized_views: vec![],
            health_indicators: CassandraTableHealthIndicators::default(),
            maintenance_info: CassandraTableMaintenanceInfo::default(),
            created_at: None,
            last_modified: None,
        };

        table_info.table_details = vec![good_table];
        table_info.total_tables = 1;
        table_info.total_sstables = 2;

        let score = table_info.storage_efficiency_score();
        // compression: (5.0 - 1.0) * 20.0 = 80.0
        // sstable efficiency: 100.0 - (2.0/1.0).min(20.0) * 3.0 = 94.0
        // combined: (80.0 + 94.0) / 2.0 = 87.0
        assert!(score > 80.0);
    }

    #[test]
    fn test_tables_by_column_count() {
        let mut table_info = CassandraTableInfo::default();

        for (i, column_count) in [5u64, 15, 35, 75].iter().enumerate() {
            table_info.table_details.push(CassandraTableDetail {
                keyspace_name: "test".to_string(),
                table_name: format!("table_{}", i),
                table_id: format!("id_{}", i),
                table_type: "USER".to_string(),
                column_info: CassandraTableColumnInfo { total_columns: *column_count, ..Default::default() },
                storage_metrics: CassandraTableStorageMetrics::default(),
                performance_metrics: CassandraTablePerformanceDetail::default(),
                configuration: CassandraTableConfiguration::default(),
                indexes: vec![],
                materialized_views: vec![],
                health_indicators: CassandraTableHealthIndicators::default(),
                maintenance_info: CassandraTableMaintenanceInfo::default(),
                created_at: None,
                last_modified: None,
            });
        }

        // Exercise the private helper through the public distribution stats accessor.
        let stats = table_info.get_table_distribution_stats();
        let ranges = stats.tables_by_column_count;
        assert_eq!(ranges.get("1-10"), Some(&1));
        assert_eq!(ranges.get("11-25"), Some(&1));
        assert_eq!(ranges.get("26-50"), Some(&1));
        assert_eq!(ranges.get("51+"), Some(&1));
    }

    #[test]
    fn test_recommendations_generation() {
        let mut table_info = CassandraTableInfo::default();

        table_info.health_metrics.overall_health_score = 50.0;
        table_info.health_metrics.tables_with_design_issues = 3;
        table_info.health_metrics.tables_with_performance_issues = 2;
        table_info.health_metrics.tables_with_high_tombstones = 1;
        table_info.health_metrics.tables_with_poor_compression = 2;
        table_info.maintenance_metrics.tables_needing_maintenance = 4;
        table_info.empty_tables = 6;
        table_info.health_metrics.tables_missing_indexes = 2;

        let recommendations = table_info.get_table_recommendations();

        assert!(recommendations.iter().any(|r| r.contains("CRITICAL")));
        assert!(recommendations.iter().any(|r| r.contains("design issues")));
        assert!(recommendations.iter().any(|r| r.contains("performance issues")));
        assert!(recommendations.iter().any(|r| r.contains("tombstone")));
        assert!(recommendations.iter().any(|r| r.contains("compression")));
        assert!(recommendations.iter().any(|r| r.contains("maintenance")));
        assert!(recommendations.iter().any(|r| r.contains("empty tables")));
        assert!(recommendations.iter().any(|r| r.contains("indexes")));
    }

    #[test]
    fn test_request_contains_valid_queries() {
        let info = CassandraTableInfo::default();
        let requests = info.request();

        // Required standard-table queries must be present.
        assert!(requests.contains_key("tables"));
        assert!(requests.contains_key("columns"));
        assert!(requests.contains_key("indexes"));
        assert!(requests.contains_key("views"));
        assert!(requests.contains_key("size_estimates"));
        assert!(requests.contains_key("compaction_history"));

        // Non-standard tables must be absent.
        assert!(!requests.contains_key("sstable_activity"));
        assert!(!requests.contains_key("snapshots"));
    }

    #[test]
    fn test_no_fabricated_performance_data() {
        // Verify that the default performance detail is all-zero.
        let perf = CassandraTablePerformanceDetail::default();
        assert_eq!(perf.read_ops_per_sec, 0.0);
        assert_eq!(perf.write_ops_per_sec, 0.0);
        assert_eq!(perf.avg_read_latency_ms, 0.0);
        assert!(!perf.has_hot_partitions);
        assert_eq!(perf.performance_score, 0.0);
    }
}
