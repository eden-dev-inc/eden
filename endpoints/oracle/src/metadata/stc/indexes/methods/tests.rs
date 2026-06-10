use super::*;

#[test]
fn test_index_summary_and_recommendations() {
    let indexes = vec![
        OracleIndexInfo {
            index_size_bytes: 1_000_000,
            fragmentation_level: 20.0,
            usage_score: 80.0,
            needs_rebuild: false,
            stale_statistics: false,
            is_partitioned: true,
            compression: "ENABLED".to_string(),
            uniqueness: "UNIQUE".to_string(),
            visibility: "VISIBLE".to_string(),
            status: "VALID".to_string(),
            ..Default::default()
        },
        OracleIndexInfo {
            index_size_bytes: 500_000,
            fragmentation_level: 60.0,
            usage_score: 0.0,
            needs_rebuild: true,
            stale_statistics: true,
            is_partitioned: false,
            compression: "DISABLED".to_string(),
            uniqueness: "NONUNIQUE".to_string(),
            visibility: "INVISIBLE".to_string(),
            status: "VALID".to_string(),
            ..Default::default()
        },
    ];

    let summary = OracleIndexSummary::from_indexes(&indexes);
    assert_eq!(summary.total_indexes, 2);
    assert_eq!(summary.rebuild_needed, 1);
    assert_eq!(summary.total_index_size, 1_500_000);
    assert_eq!(summary.healthy_percentage(), 50.0);

    let mut index = indexes[0].clone();
    index.needs_rebuild = true;
    index.rebuild_reason = Some("High B-tree depth (5)".to_string());
    index.column_count = 2;
    index.stale_statistics = true;

    let recommendations = index.maintenance_recommendations();
    assert!(recommendations.iter().any(|r| r.contains("Rebuild index")));
    assert!(recommendations.iter().any(|r| r.contains("statistics")));
}
