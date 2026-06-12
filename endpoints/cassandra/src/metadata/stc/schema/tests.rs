use super::super::utils::row_count;
use super::*;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn test_schema_agreement_detection() {
    let mut schema_info = CassandraSchemaInfo::default();

    schema_info.schema_versions.insert("abc123".to_string(), 5);
    schema_info.schema_agreement = true;
    assert!(schema_info.schema_agreement);
    assert_eq!(schema_info.nodes_with_disagreement, 0);

    schema_info.schema_versions.insert("def456".to_string(), 2);
    schema_info.schema_agreement = false;
    schema_info.nodes_with_disagreement = 2;

    assert!(!schema_info.schema_agreement);
    assert_eq!(schema_info.nodes_with_disagreement, 2);
}

#[test]
fn test_complexity_rating() {
    let mut schema_info = CassandraSchemaInfo::default();

    schema_info.complexity_metrics.complexity_score = 15.0;
    assert_eq!(schema_info.complexity_rating(), "A");

    schema_info.complexity_metrics.complexity_score = 35.0;
    assert_eq!(schema_info.complexity_rating(), "B");

    schema_info.complexity_metrics.complexity_score = 55.0;
    assert_eq!(schema_info.complexity_rating(), "C");

    schema_info.complexity_metrics.complexity_score = 75.0;
    assert_eq!(schema_info.complexity_rating(), "D");

    schema_info.complexity_metrics.complexity_score = 95.0;
    assert_eq!(schema_info.complexity_rating(), "F");
}

#[test]
fn test_health_rating() {
    let mut schema_info = CassandraSchemaInfo::default();

    schema_info.health_metrics.health_score = 95.0;
    assert_eq!(schema_info.health_rating(), "A");

    schema_info.health_metrics.health_score = 85.0;
    assert_eq!(schema_info.health_rating(), "B");

    schema_info.health_metrics.health_score = 75.0;
    assert_eq!(schema_info.health_rating(), "C");

    schema_info.health_metrics.health_score = 65.0;
    assert_eq!(schema_info.health_rating(), "D");

    schema_info.health_metrics.health_score = 45.0;
    assert_eq!(schema_info.health_rating(), "F");
}

#[test]
fn test_critical_issues_detection() {
    let mut schema_info = CassandraSchemaInfo { schema_agreement: true, ..Default::default() };

    schema_info.health_metrics.health_score = 80.0;
    schema_info.complexity_metrics.complexity_score = 60.0;
    schema_info.health_metrics.security_issues_count = 0;
    assert!(!schema_info.has_critical_schema_issues());

    schema_info.schema_agreement = false;
    assert!(schema_info.has_critical_schema_issues());

    schema_info.schema_agreement = true;
    schema_info.health_metrics.health_score = 40.0;
    assert!(schema_info.has_critical_schema_issues());

    schema_info.health_metrics.health_score = 80.0;
    schema_info.complexity_metrics.complexity_score = 90.0;
    assert!(schema_info.has_critical_schema_issues());

    schema_info.complexity_metrics.complexity_score = 60.0;
    schema_info.health_metrics.security_issues_count = 1;
    assert!(schema_info.has_critical_schema_issues());
}

#[test]
fn test_naming_convention_check() {
    assert!(has_good_naming("user_profiles"));
    assert!(has_good_naming("order_items_v2"));
    assert!(has_good_naming("simple"));

    assert!(!has_good_naming("UserProfiles"));
    assert!(!has_good_naming("user-profiles"));
    assert!(!has_good_naming("_private"));
    assert!(!has_good_naming("temp_"));
}

#[test]
fn test_partition_design_validation() {
    let make_table = |pk: Vec<&str>| CassandraTableDetail {
        keyspace_name: "test".to_string(),
        table_name: "test_table".to_string(),
        table_id: "123".to_string(),
        columns: vec![],
        partition_key: pk.into_iter().map(|s| s.to_string()).collect(),
        clustering_key: vec![],
        static_columns: vec![],
        compaction_strategy: "STCS".to_string(),
        compression_algorithm: "LZ4".to_string(),
        caching_config: HashMap::new(),
        bloom_filter_fp_chance: 0.01,
        default_ttl: None,
        gc_grace_seconds: 864000,
        comment: None,
        flags: vec![],
        index_count: 0,
        has_materialized_views: false,
        schema_version: None,
    };

    assert!(has_good_partition_design(&make_table(vec!["user_id"])));
    assert!(has_good_partition_design(&make_table(vec!["user_id", "tenant_id"])));
    assert!(has_good_partition_design(&make_table(vec!["user_id", "tenant_id", "region"])));
    assert!(!has_good_partition_design(&make_table(vec!["a", "b", "c", "d"])));
    assert!(!has_good_partition_design(&make_table(vec![])));
}

#[test]
fn test_anti_pattern_detection() {
    let make_col = |name: &str| CassandraColumnDetail {
        column_name: name.to_string(),
        column_type: "text".to_string(),
        column_kind: "regular".to_string(),
        position: None,
        clustering_order: None,
        is_frozen: false,
        type_arguments: vec![],
    };

    let mut table = CassandraTableDetail {
        keyspace_name: "test".to_string(),
        table_name: "test_table".to_string(),
        table_id: "123".to_string(),
        columns: (0..10).map(|i| make_col(&format!("col_{i}"))).collect(),
        partition_key: vec!["user_id".to_string()],
        clustering_key: vec![],
        static_columns: vec![],
        compaction_strategy: "STCS".to_string(),
        compression_algorithm: "LZ4".to_string(),
        caching_config: HashMap::new(),
        bloom_filter_fp_chance: 0.01,
        default_ttl: None,
        gc_grace_seconds: 864000,
        comment: None,
        flags: vec![],
        index_count: 0,
        has_materialized_views: false,
        schema_version: None,
    };

    assert!(!has_anti_patterns(&table));

    // Too many columns
    table.columns = (0..150).map(|i| make_col(&format!("col_{i}"))).collect();
    assert!(has_anti_patterns(&table));

    // Too many clustering columns
    table.columns = vec![make_col("test_col")];
    table.clustering_key = (0..15).map(|i| format!("cluster_col_{i}")).collect();
    assert!(has_anti_patterns(&table));
}

#[test]
fn test_collection_type_detection() {
    assert!(is_collection_type("map<text, int>"));
    assert!(is_collection_type("set<uuid>"));
    assert!(is_collection_type("list<text>"));
    assert!(is_collection_type("frozen<map<text, frozen<user_type>>>"));

    assert!(!is_collection_type("text"));
    assert!(!is_collection_type("int"));
    assert!(!is_collection_type("uuid"));
    assert!(!is_collection_type("user_defined_type"));
}

#[test]
fn test_primitive_type_detection() {
    assert!(is_primitive_type("text"));
    assert!(is_primitive_type("int"));
    assert!(is_primitive_type("uuid"));
    assert!(is_primitive_type("timestamp"));
    assert!(is_primitive_type("boolean"));

    assert!(!is_primitive_type("user_type"));
    assert!(!is_primitive_type("map<text, int>"));
    assert!(!is_primitive_type("frozen<user_type>"));
}

#[test]
fn test_udt_extraction() {
    assert_eq!(extract_udt_from_type("user_profile"), Some("user_profile".to_string()));
    assert_eq!(extract_udt_from_type("address_info"), Some("address_info".to_string()));

    assert_eq!(extract_udt_from_type("text"), None);
    assert_eq!(extract_udt_from_type("map<text, int>"), None);
    assert_eq!(extract_udt_from_type("list<uuid>"), None);
}

#[test]
fn test_complexity_score_calculation() {
    let metrics = CassandraSchemaComplexityMetrics {
        avg_columns_per_table: 10.0,
        avg_partition_key_complexity: 1.5,
        avg_clustering_key_complexity: 2.0,
        tables_with_composite_keys: 5,
        tables_with_static_columns: 2,
        tables_with_collections: 3,
        avg_indexes_per_table: 1.2,
        ..Default::default()
    };

    let score = calculate_complexity_score(&metrics, &[]);
    assert!((0.0..=100.0).contains(&score));
}

#[test]
fn test_extract_key_columns() {
    let columns = vec![
        CassandraColumnDetail {
            column_name: "user_id".to_string(),
            column_type: "uuid".to_string(),
            column_kind: "partition_key".to_string(),
            position: Some(0),
            clustering_order: None,
            is_frozen: false,
            type_arguments: vec![],
        },
        CassandraColumnDetail {
            column_name: "created_at".to_string(),
            column_type: "timestamp".to_string(),
            column_kind: "clustering".to_string(),
            position: Some(0),
            clustering_order: Some("DESC".to_string()),
            is_frozen: false,
            type_arguments: vec![],
        },
        CassandraColumnDetail {
            column_name: "data".to_string(),
            column_type: "text".to_string(),
            column_kind: "static".to_string(),
            position: None,
            clustering_order: None,
            is_frozen: false,
            type_arguments: vec![],
        },
        CassandraColumnDetail {
            column_name: "name".to_string(),
            column_type: "text".to_string(),
            column_kind: "regular".to_string(),
            position: None,
            clustering_order: None,
            is_frozen: false,
            type_arguments: vec![],
        },
    ];

    let (partition_key, clustering_key, static_columns) = extract_key_columns(&columns);
    assert_eq!(partition_key, vec!["user_id"]);
    assert_eq!(clustering_key, vec!["created_at"]);
    assert_eq!(static_columns, vec!["data"]);
}

#[test]
fn test_schema_recommendations() {
    let mut schema_info = CassandraSchemaInfo { schema_agreement: false, ..Default::default() };

    schema_info.complexity_metrics.complexity_score = 80.0;
    schema_info.health_metrics.health_score = 60.0;
    schema_info.complexity_metrics.tables_with_excessive_columns = 3;
    schema_info.health_metrics.tables_with_anti_patterns = 2;
    schema_info.health_metrics.security_issues_count = 1;
    schema_info.evolution_metrics.stability_score = 70.0;

    let recommendations = schema_info.get_schema_recommendations();

    assert!(recommendations.len() > 5);
    assert!(recommendations.iter().any(|r| r.contains("CRITICAL")));
    assert!(recommendations.iter().any(|r| r.contains("complexity")));
    assert!(recommendations.iter().any(|r| r.contains("health")));
    assert!(recommendations.iter().any(|r| r.contains("excessive columns")));
    assert!(recommendations.iter().any(|r| r.contains("anti-patterns")));
    assert!(recommendations.iter().any(|r| r.contains("Security")));
}

#[test]
fn test_count_rows() {
    let data_array = json!([
        {"key": "value1"},
        {"key": "value2"},
        {"key": "value3"}
    ]);
    assert_eq!(row_count(&data_array), 3);

    let data_object = json!({"key": "value"});
    assert_eq!(row_count(&data_object), 0);

    let data_empty = json!([]);
    assert_eq!(row_count(&data_empty), 0);
}

#[test]
fn test_tables_by_column_ranges() {
    let make_col = |name: &str| CassandraColumnDetail {
        column_name: name.to_string(),
        column_type: "text".to_string(),
        column_kind: "regular".to_string(),
        position: None,
        clustering_order: None,
        is_frozen: false,
        type_arguments: vec![],
    };

    let mut schema_info = CassandraSchemaInfo::default();

    for (i, col_count) in [5usize, 15, 35, 75, 8].iter().enumerate() {
        schema_info.table_details.push(CassandraTableDetail {
            keyspace_name: "test".to_string(),
            table_name: format!("table_{i}"),
            table_id: format!("id_{i}"),
            columns: (0..*col_count).map(|j| make_col(&format!("col_{j}"))).collect(),
            partition_key: vec![],
            clustering_key: vec![],
            static_columns: vec![],
            compaction_strategy: "STCS".to_string(),
            compression_algorithm: "LZ4".to_string(),
            caching_config: HashMap::new(),
            bloom_filter_fp_chance: 0.01,
            default_ttl: None,
            gc_grace_seconds: 864000,
            comment: None,
            flags: vec![],
            index_count: 0,
            has_materialized_views: false,
            schema_version: None,
        });
    }

    let ranges = schema_info.get_tables_by_column_ranges();
    assert_eq!(ranges.get("1-10"), Some(&2));
    assert_eq!(ranges.get("11-25"), Some(&1));
    assert_eq!(ranges.get("26-50"), Some(&1));
    assert_eq!(ranges.get("51+"), Some(&1));
}

#[test]
fn test_merge_peer_and_local_rows() {
    let peers = json!([
        {"peer": "10.0.0.1", "schema_version": "v1"},
        {"peer": "10.0.0.2", "schema_version": "v1"}
    ]);
    let local = json!([{"peer": "10.0.0.3", "schema_version": "v2"}]);

    let merged = merge_peer_and_local_rows(&peers, &local);
    assert_eq!(row_count(&merged), 3);
}

#[test]
fn test_evolution_metrics_no_fabrication() {
    // When schema is in agreement, evolution metrics should not invent
    // counts for schema changes; all counters should remain zero.
    let schema_info = CassandraSchemaInfo { schema_agreement: true, ..Default::default() };
    let metrics = calculate_evolution_metrics(&schema_info);

    assert_eq!(metrics.recent_schema_changes, 0);
    assert_eq!(metrics.tables_created_recently, 0);
    assert_eq!(metrics.indexes_created_recently, 0);
    assert_eq!(metrics.types_created_recently, 0);
    assert_eq!(metrics.change_frequency_per_week, 0.0);
    assert_eq!(metrics.stability_score, 100.0);
    assert_eq!(metrics.version_drift_days, 0.0);
}
